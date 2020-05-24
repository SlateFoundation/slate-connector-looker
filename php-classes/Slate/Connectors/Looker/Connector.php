<?php

namespace Slate\Connectors\Looker;

use Psr\Log\LogLevel;
use Psr\Log\LoggerInterface;

use Slate;
use Slate\People\Student;

use Slate\Connectors\Looker\API AS LookerAPI;

use Emergence\Connectors\AbstractConnector;
use Emergence\Connectors\ISynchronize;
use Emergence\Connectors\IJob;
use Emergence\Connectors\Mapping;
use Emergence\Connectors\Exceptions\SyncException;
use Emergence\Connectors\SyncResult;

use Emergence\People\IPerson;
use Emergence\People\User;
use Emergence\People\ContactPoint\Email AS EmailContactPoint;
use Emergence\Util\Data AS DataUtil;

class Connector extends AbstractConnector implements ISynchronize
{
    public static $title = 'Looker';
    public static $connectorId = 'looker';

    public static $groupsByAccountLevel = [];
    public static $groupsByAccountType = [
        Student::class => [],
        User::class => []
    ];
    public static $groupsByUser;

    public static $rolesByAccountLevel = [];
    public static $rolesByAccountType = [
        Student::class => [],
        User::class => []
    ];
    public static $rolesByUser;

    public static $customAttributesByType = [
    ];
    public static $customAttributesByUser;

    // workflow implementations
    protected static function _getJobConfig(array $requestData)
    {
        $config = parent::_getJobConfig($requestData);

        $config['clientId'] = $requestData['clientId'];
        $config['clientSecret'] = $requestData['clientSecret'];
        $config['pushUsers'] = !empty($requestData['pushUsers']);

        return $config;
    }

    public static function synchronize(IJob $Job, $pretend = true)
    {
        if ($Job->Status != 'Pending' && $Job->Status != 'Completed') {
            return static::throwError('Cannot execute job, status is not Pending or Complete');
        }

        if (empty($Job->Config['clientId'])) {
            return static::throwError('Cannot execute job, clientId not provided');
        }

        if (empty($Job->Config['clientSecret'])) {
            return static::throwError('Cannot execute job, clientSecret not provided');
        }


        // configure API wrapper
        try {
            LookerAPI::login($Job->Config['clientId'], $Job->Config['clientSecret']);
        } catch (\Exception $e) {
            return static::throwError($e->getMessage());
        }

        // update job status
        $Job->Status = 'Pending';

        if (!$pretend) {
            $Job->save();
        }


        // init results struct
        $results = [];


        // uncap execution time
        set_time_limit(0);


        // execute tasks based on available spreadsheets
        if (!empty($Job->Config['pushUsers'])) {
            $results['push-users'] = static::pushUsers(
                $Job,
                $pretend
            );
        }


        // save job results
        $Job->Status = 'Completed';
        $Job->Results = $results;

        if (!$pretend) {
            $Job->save();
        }

        return true;
    }

    public static function pushUser(IPerson $User, LoggerInterface $logger = null, $pretend = true)
    {
        $logger = static::getLogger($logger);

        // get mapping
        $mappingData = [
            'ContextClass' => $User->getRootClass(),
            'ContextID' => $User->ID,
            'Connector' => static::getConnectorId(),
            'ExternalKey' => 'user[id]'
        ];

        $Mapping = Mapping::getByWhere($mappingData);

        // sync account
        if ($Mapping) {
            $logger->debug(
                'Found mapping to Looker user {lookerUserId}, checking for updates...',
                [
                    'lookerUserMapping' => $Mapping,
                    'lookerUserId' => $Mapping->ExternalIdentifier
                ]
            );

            // check for any changes
            $lookerUser = LookerAPI::getUserById($Mapping->ExternalIdentifier, ['fields' => 'id,first_name,last_name,email,group_ids,role_ids']);
            $changes = [];
            $lookerUserChanges = [];

            if ($lookerUser['first_name'] != ($User->PreferredName ?: $User->FirstName)) {
                $lookerUserChanges['user[first_name]'] = [
                    'from' => $lookerUser['first_name'],
                    'to' => $User->PreferredName ?: $User->Firstname
                ];
            }

            if ($lookerUser['last_name'] != $User->LastName) {
                $lookerUserChanges['user[last_name]'] = [
                    'from' => $lookerUser['last_name'],
                    'to' => $User->Lastname
                ];
            }

            // sync changes with API
            if (!empty($lookerUserChanges)) {
                if (!$pretend) {
                    $lookerResponse = LookerAPI::updateUser($Mapping->ExternalIdentifier, DataUtil::extractToFromDelta($lookerUserChanges));
                    $logger->debug(
                        'Updating Looker for user {slateUsername}',
                        [
                            'slateUsername' => $User->Username,
                            'lookerUserChanges' => $lookerUserChanges,
                            'lookerResponse' => $lookerResponse
                        ]
                    );
                }
                $logger->notice(
                    'Updated user {slateUsername}',
                    [
                        'slateUsername' => $User->Username,
                        'changes' => $changes['user']
                    ]
                );
            } else {
                $logger->debug(
                    'Looker user matches Slate user {slateUsername}',
                    [
                        'slateUsername' => $User->Username
                    ]
                );
            }


            $userUpdated = false;
            // sync groups
            try {
                $groupSyncResult = static::syncUserGroups($User, $lookerUser, $logger, $pretend);
                if ($groupSyncResult->getStatus() === SyncResult::STATUS_UPDATED) {
                    $userUpdated = true;
                }
            } catch (SyncException $e) {
                $logger->error(
                    $e->getInterpolatedMessage(),
                    $e->getContext()
                );
            }
            // sync roles
            try {
                $rolesSyncResult = static::syncUserRoles($User, $lookerUser, $logger, $pretend);
                if ($rolesSyncResult->getStatus() === SyncResult::STATUS_UPDATED) {
                    $userUpdated = true;
                }
            } catch (SyncException $e) {
                $logger->error(
                    $e->getInterpolatedMessage(),
                    $e->getContext()
                );
            }
            // sync custom attributes
            try {
                $customAttributesSyncResult = static::syncUserCustomAttributes($User, $lookerUser, $logger, $pretend);
                if ($customAttributesSyncResult->getStatus() === SyncResult::STATUS_UPDATED) {
                    $userUpdated = true;
                }
            } catch (SyncException $e) {
                $logger->error(
                    $e->getInterpolatedMessage(),
                    $e->getContext()
                );
            }

            return new SyncResult(
                (!empty($changes) || !empty($userUpdated)) ? SyncResult::STATUS_UPDATED : SyncResult::STATUS_VERIFIED,
                'Looker account for {slateUsername} found and verified up-to-date.',
                [
                    'slateUsername' => $User->Username
                ]
            );
        } else { // try to create user if no mapping found
            // skip accounts with no email
            if (!$User->PrimaryEmail) {
                $logger->debug(
                    'Skipping user {slateUsername} without Primary Email',
                    [
                        'slateUsername' => $User->Username
                    ]
                );

                return new SyncResult(
                    SyncResult::STATUS_SKIPPED,
                    'No email, skipping {slateUsername}',
                    [
                        'slateUsername' => $User->Username
                    ]
                );
            }

            if (!$pretend) {
                $lookerResponse = LookerAPI::createUser([
                    'first_name' => $User->FirstName,
                    'last_name' => $User->LastName,
                    'email' => $User->PrimaryEmail->toString()
                ]);

                if (empty($lookerResponse['id'])) {
                    throw new SyncException(
                        'Failed to create Looker user for {slateUsername}',
                        [
                            'slateUsername' => $User->Username,
                            'lookerResponse' => $lookerResponse
                        ]
                    );
                }

                $mappingData['ExternalIdentifier'] = $lookerResponse['id'];
                Mapping::create($mappingData, true);
            } else {
                $lookerResponse = [];
            }

            $logger->notice(
                'Created Looker user for {slateUsername}',
                    [
                        'slateUsername' => $User->Username,
                        'lookerResponse' => $pretend ? '(pretend-mode)' : $lookerResponse
                    ]
                );

            // sync groups
            try {
                $groupSyncResult = static::syncUserGroups($User, [], $logger, $pretend);
            } catch (SyncException $e) {
                $logger->error(
                    $e->getInterpolatedMessage(),
                    $e->getContext()
                );
            }
            // sync roles
            try {
                static::syncUserRoles($User, [], $logger, $pretend);
            } catch (SyncException $e) {
                $logger->error(
                    $e->getInterpolatedMessage(),
                    $e->getContext()
                );
            }
            // sync custom attributes
            try {
                $customAttributesSyncResult = static::syncUserCustomAttributes($User, $lookerResponse, $logger, $pretend);
            } catch (SyncException $e) {
                $logger->error(
                    $e->getInterpolatedMessage(),
                    $e->getContext()
                );
            }

            return new SyncResult(
                SyncResult::STATUS_CREATED,
                'Created Looker user for {slateUsername}, saved mapping to new Looker user #{lookerUserId}',
                    [
                        'slateUsername' => $User->Username,
                        'lookerUserId' => $pretend ? '(pretend-mode)' : $lookerResponse['id'],
                        'lookerUser' => $pretend ? '(pretend-mode)' : $lookerResponse
                    ]
                );


        }
    }

    protected static function getUserRoles(IPerson $User)
    {
        $roleIds = [];
        if (isset(static::$rolesByAccountLevel[$User->AccountLevel])) {
            $roleIds = array_merge($roleIds, static::$rolesByAccountLevel[$User->AccountLevel]);
        }

        if (isset(static::$rolesByAccountType[$User->Class])) {
            $roleIds = array_merge($roleIds, static::$rolesByAccountType[$User->Class]);
        }

        if (isset(static::$rolesByUser) && is_callable(static::$rolesByUser)) {
            $roleIds = array_merge($roleIds, call_user_func(static::$rolesByUser, $User));
        }

        return $roleIds;
    }

    protected static function syncUserRoles(IPerson $User, array $lookerUser, LoggerInterface $logger = null, $pretend = true)
    {
        $roleIds = $lookerUser['role_ids'] ?: [];
        $userRoles = static::getUserRoles($User);

        $logger->debug(
            'Analyzing user roles: [{roleIds}]',
            [
                'roleIds' => join(',', $roleIds)
            ]
        );

        if ($rolesToAdd = array_diff($userRoles, $roleIds)) {
            $logger->debug(
                'Updating {slateUsername} roles to: [{userRoles}]',
                [
                    'slateUsername' => $User->Username,
                    'userRoles' => join(',', array_unique(array_merge($userRoles, $roleIds)))
                ]
            );

            if (!$pretend) {
                $lookerResponse = LookerAPI::updateUserRoles($lookerUser['id'], $rolesToAdd);
                $userRoleIds = [];
                foreach ($lookerResponse as $userRoleData) {
                    $userRoleIds[] = $userRoleData['id'];
                }

                if (empty($userRoleIds) || array_diff($userRoleIds, $rolesToAdd)) {
                    $logger->error('Error syncing user roles', [
                        'lookerResponse' => $lookerResponse,
                        'rolesToAdd' => $rolesToAdd
                    ]);

                    return new SyncException(
                        'Unable to sync user roles.',
                        [
                            'lookerResponse' => $lookerResponse
                        ]
                    );
                }

                $logger->debug(
                    'User roles successfully synced.',
                    [
                        'lookerResponse' => $lookerResponse
                    ]
                );
            }
        } else {
            $logger->debug(
                'User roles verified',
                [
                    'local' => $rolesToAdd,
                    'remote' => $roleIds
                ]
            );
        }

        return new SyncResult(
            empty($rolesToAdd) ? SyncResult::STATUS_VERIFIED : SyncResult::STATUS_UPDATED,
            'Successfully synced user roles with Looker',
            [
                'lookerResponse' => $looekrResponse
            ]
        );
    }

    protected static function getUserGroups(IPerson $User)
    {
        $groupIds = [];
        if (isset(static::$groupsByAccountLevel[$User->AccountLevel])) {
            $groupIds = array_merge($groupIds, static::$groupsByAccountLevel[$User->AccountLevel]);
        }

        if (isset(static::$groupsByAccountType[$User->Class])) {
            $groupIds = array_merge($groupIds, static::$groupsByAccountType[$User->Class]);
        }

        if (isset(static::$groupsByUser) && is_callable(static::$groupsByUser)) {
            $groupIds = array_merge($groupIds, call_user_func(static::$groupsByUser, $User));
        }
        return $groupIds;
    }

    protected static function syncUserGroups(IPerson $User, array $lookerUser, LoggerInterface $logger = null, $pretend = true)
    {
        $groupIds = $lookerUser['group_ids'] ?: [];
        $userGroups = array_values(static::getUserGroups($User));
        $logger->debug(
            'Analyzing user groups: [{groupIds}]',
            [
                'groupIds' => join(',', $groupIds)
            ]
        );

        if ($groupsToAdd = array_diff($userGroups, $groupIds)) {
            $logger->debug(
                'Updating {slateUsername} groups to: [{userGroups}]',
                [
                    'slateUsername' => $User->Username,
                    'userGroups' => join(',', array_unique(array_merge($userGroups, $groupIds)))
                ]
            );

            // sync user groups via API
            foreach ($groupsToAdd as $groupId) {
                if ($pretend) {
                    $logger->debug(
                        'Adding user to group with ID: {groupId}',
                        [
                            'groupId' => $groupId
                        ]
                    );
                    continue;
                }

                $lookerResponse = LookerAPI::addUserToGroup($lookerUser['id'], $groupId);

                if (!empty($lookerResponse['group_ids']) && in_array($groupId, $lookerResponse['group_ids'])) {
                    $logger->debug(
                        'Added user to group with ID: {groupId}',
                        [
                            'lookerResponse' => $lookerResponse,
                            'groupId' => $groupId
                        ]
                    );
                } else {
                    $logger->error(
                        'Error adding user to group with ID: {groupId}',
                        [
                            'groupId' => $groupId,
                            'lookerResponse' => $lookerResponse
                        ]
                    );
                }
            }
        } else {
            $logger->debug(
                'User groups verified',
                [
                    'remote' => $groupIds,
                    'local' => $userGroups
                ]
            );
        }

        return new SyncResult(
            empty($groupsToAdd) ? SyncResult::STATUS_VERIFIED : SyncResult::STATUS_UPDATED,
            'Updated groups for {slateUsername} in Looker ' . $pretend ? '(pretend-mode)' : '',
            [
                'slateUsername' => $User->Username
            ]
        );
    }

    protected static function getUserCustomAttributes(IPerson $User)
    {
        $customAttributes = [];

        if (isset(static::$customAttributesByType[$User->Class])) {
            $customAttributesByType = static::$customAttributesByType[$User->Class];
            foreach ($customAttributesByType as $customAttributeId => $customAttributeMapping) {
                if (isset($customAttributeMapping['getter'])) {
                    $customAttributes[$customAttributeId] = $User->getValue($customAttributeMapping['getter']);
                } else if (isset($customAttributeMapping['value'])) {
                    $customAttributes[$customAttributeId] = $customAttributeMapping['value'];
                }
            }
        }

        if (isset(static::$customAttributesByUser) && is_callable(static::$customAttributesByUser)) {
            $customAttributes = array_merge($customAttributes, call_user_func(static::$customAttributesByUser, $User));
        }

        return $customAttributes;
    }

    protected static function syncUserCustomAttributes(IPerson $User, array $lookerUser, LoggerInterface $logger = null, $pretend = true)
    {
        $userCustomAttributes = static::getUserCustomAttributes($User);
        $customAttributesToAdd = [];

        if (!empty($lookerUser)) {
            $currentUserCustomAttributes = LookerAPI::getUserCustomAttributes($lookerUser['id']);
        } else {
            $currentUserCustomAttributes = [];
        }

        if (!empty($currentUserCustomAttributes)) {
            foreach ($currentUserCustomAttributes as $lookerCustomAttribute) {
                if (!array_key_exists($lookerCustomAttribute['name'], $userCustomAttributes)) {
                    continue;
                }

                if ($lookerCustomAttribute['value'] != $userCustomAttributes[$lookerCustomAttribute['name']]) {
                    $customAttributesToAdd[$lookerCustomAttribute['user_attribute_id']] = [
                        'name' => $lookerCustomAttribute['name'],
                        'value' => $userCustomAttributes[$lookerCustomAttribute['name']]
                    ];
                }
            }
        }

        if (empty($customAttributesToAdd)) {
            $logger->debug(
                'User Custom Attributes verified',
                [
                    'local' => $customAttributesToAdd,
                    'remote' => $userCustomAttributes
                ]
            );
            return new SyncResult(
                SyncResult::STATUS_VERIFIED,
                'User custome attributes have been verified.',
                [
                    'lookerUserAttributes' => $currentUserCustomAttributes
                ]
            );
        } else {

            $logger->debug('Syncing user custom attributes for {slateUsername}: {attributesToSet}', [
                'slateUsername' => $User->Username,
                'attributesToSet' => join(', ', array_map(function($c) { return "$c[name] => $c[value]"; }, $customAttributesToAdd))
            ]);

            foreach ($customAttributesToAdd as $customAttributeId => $customAttributeToAdd) {
                if (!$pretend) {
                    $lookerResponse = LookerAPI::updateUserCustomAttribute($lookerUser['id'], $customAttributeId, $customAttributeToAdd);
                    if ($lookerResponse['value'] != $customAttributeToAdd['value']) {
                        \MICS::dump($lookerResponse, 'looker response');
                        $logger->error(
                            'Error updating user Custom Attribute {customAttributeName} => {customAttributeValue}',
                            [
                                'customAttributeName' => $customAttributeToAdd['name'],
                                'customAttributeValue' => $customAttributeToAdd['value'],
                                'lookerResponse' => $lookerResponse
                            ]
                        );
                        continue;
                    }
                }

                $logger->debug(
                    'Synced user Custom Attribute {customAttributeName} => {customAttributeValue}',
                    [
                        'customAttributeName' => $customAttributeToAdd['name'],
                        'customAttributeValue' => $customAttributeToAdd['value'],
                        'lookerResponse' => $lookerResponse
                    ]
                );
            }


            return new SyncResult(
                SyncResult::STATUS_UPDATED,
                'User custom attributes have been updated.' . ($pretend ? '(pretend-mode)' : ''),
                [
                    'previousAttributes' => $currentUserCustomAttributes,
                    'lookerResponse' => $pretend ? '(pretend-mode)' : $lookerResponse
                ]
            );
        }
    }

    // task handlers
    public static function pushUsers(IJob $Job, $pretend = true)
    {
        // initialize results
        $results = [
            'analyzed' => 0,
            'created' => 0,
            'updated' => 0,
            'skipped' => 0,
            'failed' => 0
        ];

        // iterate over Slate users
        $slateUsers = [];
        $slateOnlyUsers = [];

        foreach (User::getAllByWhere('Username IS NOT NULL AND AccountLevel != "Disabled"') AS $User) {
            $Job->debug(
                'Analyzing Slate user {slateUsername} ({slateUserClass}/{userGraduationYear})',
                [
                    'slateUsername' => $User->Username,
                    'slateUserClass' => $User->Class,
                    'userGraduationYear' => $User->GraduationYear
                ]
            );
            $results['analyzed']++;

            try {
                $syncResult = static::pushUser($User, $Job, $pretend);

                if ($syncResult->getStatus() === SyncResult::STATUS_CREATED) {
                    $results['created']++;
                } elseif ($syncResult->getStatus() === SyncResult::STATUS_UPDATED) {
                    $results['updated']++;
                } elseif ($syncResult->getStatus() === SyncResult::STATUS_SKIPPED) {
                    $results['skipped']++;
                    continue;
                }
            } catch (SyncException $e) {
                $Job->logException($e);
                $results['failed']++;
            }
        } // end of Slate users loop

        return $results;
    }
}