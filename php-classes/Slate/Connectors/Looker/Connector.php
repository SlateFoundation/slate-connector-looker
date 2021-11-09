<?php

namespace Slate\Connectors\Looker;

use Psr\Log\LogLevel;
use Psr\Log\LoggerInterface;

use Slate;
use Slate\People\Student;

use Slate\Connectors\Looker\API AS LookerAPI;

use Emergence\Connectors\ISynchronize;
use Emergence\Connectors\IJob;
use Emergence\Connectors\Mapping;
use Emergence\Connectors\Exceptions\SyncException;
use Emergence\Connectors\SyncResult;
use Emergence\SAML2\Connector as SAML2Connector;

use Emergence\People\IPerson;
use Emergence\People\User;
use Emergence\People\ContactPoint\Email AS EmailContactPoint;
use Emergence\Util\Data AS DataUtil;

use Slate\NetworkHub\SchoolUser;

class Connector extends SAML2Connector implements ISynchronize
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
        $config['pushSchools'] = $requestData['schools'];

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

        // sync account
        if ($Mapping = static::getUserMapping($User)) {
            $logger->debug(
                'Found mapping to Looker user {lookerUserId}, checking for updates...',
                [
                    'lookerUserMapping' => $Mapping,
                    'lookerUserId' => $Mapping->ExternalIdentifier
                ]
            );

            // check for any changes
            $lookerUser = LookerAPI::getUserById($Mapping->ExternalIdentifier, ['fields' => 'id,first_name,last_name,email,group_ids,role_ids,is_disabled']);
            if (isset($lookerUser['message']) && $lookerUser['message'] === 'Not found') {
                throw new SyncException('Failed to find Looker User with ID '. $Mapping->ExternalIdentifier);
            }

            $lookerUserChanges = [];

            if ($lookerUser['first_name'] != $User->FirstName) {
                $lookerUserChanges['first_name'] = [
                    'from' => $lookerUser['first_name'],
                    'to' => $User->FirstName
                ];
            }

            if ($lookerUser['last_name'] != $User->LastName) {
                $lookerUserChanges['last_name'] = [
                    'from' => $lookerUser['last_name'],
                    'to' => $User->LastName
                ];
            }

            // sync slate "Disabled" account status with Looker
            $slateUserDisabled = $User->AccountLevel == 'Disabled';
            if ($lookerUser['is_disabled'] != $slateUserDisabled) {
                $lookerUserChanges['is_disabled'] = [
                    'from' => $lookerUser['is_disabled'],
                    'to' => $slateUserDisabled
                ];
            }

            // sync changes with API
            if (!empty($lookerUserChanges)) {
                if (!$pretend) {
                    $lookerResponse = LookerAPI::updateUser($Mapping->ExternalIdentifier, DataUtil::extractToFromDelta($lookerUserChanges));
                    $logger->debug(
                        'Updating Looker for user {slateEmail}',
                        [
                            'slateEmail' => $User->Email,
                            'lookerUserChanges' => $lookerUserChanges,
                            'lookerResponse' => $lookerResponse
                        ]
                    );
                }
                $logger->notice(
                    'Updated user {slateEmail}',
                    [
                        'slateEmail' => $User->Email,
                        'changes' => $changes['user']
                    ]
                );
            } else {
                $logger->debug(
                    'Looker user matches Slate user {slateEmail}',
                    [
                        'slateEmail' => $User->Email
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
                (!empty($lookerUserChanges) || !empty($userUpdated)) ? SyncResult::STATUS_UPDATED : SyncResult::STATUS_VERIFIED,
                'Looker account for {slateEmail} found and verified up-to-date.',
                [
                    'slateEmail' => $User->Email
                ]
            );
        } else { // try to create user if no mapping found
            // skip disabled accounts and accounts with no email
            if (!$User->Email) {
                $logger->debug(
                    'Skipping user {slateUsername} without Email',
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
            } else if ($User->AccountLevel == 'Disabled') {
                $logger->debug(
                    'Skipping disabled user {slateUsername}',
                    [
                        'slateUsername' => $User->Username
                    ]
                );

                return new SyncResult(
                    SyncResult::STATUS_SKIPPED,
                    'User Disabled, skipping {slateUsername}',
                    [
                        'slateUsername' => $User->Username
                    ]
                );
            }

            if (!$pretend) {
                $lookerResponse = LookerAPI::createUser([
                    'first_name' => $User->FirstName,
                    'last_name' => $User->LastName
                ]);

                if (empty($lookerResponse['id'])) {
                    throw new SyncException(
                        'Failed to create Looker user for {slateEmail}',
                        [
                            'slateEmail' => $User->Email,
                            'lookerResponse' => $lookerResponse
                        ]
                    );
                }

                $Mapping = static::createUserMapping($User, $lookerResponse['id']);

                $credentialsResponse = LookerAPI::createUserEmailCredentials($Mapping->ExternalIdentifier, [
                    'email' => $User->Email
                ]);

                $logger->debug(
                    'Created Looker user credentials for {slateEmail}',
                    [
                        'slateEmail' => $User->Email,
                        'lookerResponse' => $pretend ? '(pretend-mode)' : $credentialsResponse
                    ]
                );

            } else {
                $lookerResponse = [];
            }

            $logger->notice(
                'Created Looker user for {slateEmail}',
                [
                    'slateEmail' => $User->Email,
                    'lookerResponse' => $pretend ? '(pretend-mode)' : $lookerResponse
                ]
            );
            // sync groups
            try {
                $groupSyncResult = static::syncUserGroups($User, $lookerResponse, $logger, $pretend);
            } catch (SyncException $e) {
                $logger->error(
                    $e->getInterpolatedMessage(),
                    $e->getContext()
                );
            }
            // sync roles
            try {
                static::syncUserRoles($User, $lookerResponse, $logger, $pretend);
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
                'Created Looker user for {slateEmail}, saved mapping to new Looker user #{lookerUserId}',
                    [
                        'slateEmail' => $User->Email,
                        'lookerUserId' => $pretend ? '(pretend-mode)' : $lookerResponse['id'],
                        'lookerUser' => $pretend ? '(pretend-mode)' : $lookerResponse
                    ]
                );


        }
    }

    protected static function getUserMappingData(IPerson $User)
    {
        return [
            'ContextClass' => $User->getRootClass(),
            'ContextID' => $User->ID,
            'Connector' => static::getConnectorId(),
            'ExternalKey' => 'user[id]'
        ];
    }

    protected static function getUserMapping(IPerson $User)
    {
        return Mapping::getByWhere(static::getUserMappingData($User));
    }

    protected static function createUserMapping(IPerson $User, $externalIdentifier)
    {
        $mappingData = static::getUserMappingData($User);
        $mappingData['ExternalIdentifier'] = $externalIdentifier;

        return Mapping::create($mappingData, true);
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
                'Updating {slateEmail} roles to: [{userRoles}]',
                [
                    'slateEmail' => $User->Email,
                    'userRoles' => join(',', array_unique(array_merge($userRoles, $roleIds)))
                ]
            );

            if (!$pretend) {
                $lookerResponse = LookerAPI::updateUserRoles($lookerUser['id'], $rolesToAdd);
                if (
                    empty($lookerResponse) ||
                    !is_array($lookerResponse) ||
                    (!empty($lookerResponse['message']) && $lookerResponse['message'] == 'Not found')
                ) {
                    $logger->error('Unexpected response syncing user roles', [
                        'lookerResponse' => $lookerResponse
                    ]);

                    throw new SyncException(
                        'Unable to sync user roles.',
                        [
                            'lookerResponse' => $lookerResponse
                        ]
                    );
                }

                $userRoleIds = [];
                foreach ($lookerResponse as $userRoleData) {
                    $userRoleIds[] = $userRoleData['id'];
                }

                if (empty($userRoleIds) || array_diff($userRoleIds, $rolesToAdd)) {
                    $logger->error('Error syncing user roles', [
                        'lookerResponse' => $lookerResponse,
                        'rolesToAdd' => $rolesToAdd
                    ]);

                    throw new SyncException(
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
            'Analyzing looker user groups: {groupIds}',
            [
                'groupIds' => empty($groupIds) ? '(none)' : '[' . join(',', $groupIds) .']'
            ]
        );

        if (!empty($groupsToAdd = array_diff($userGroups, $groupIds))) {

            $logger->debug(
                'Updating {slateEmail} Looker groups to: [{userGroups}]',
                [
                    'slateEmail' => $User->Email,
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
            'Updated groups for {slateEmail} in Looker ' . $pretend ? '(pretend-mode)' : '',
            [
                'slateEmail' => $User->Email
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
            $customAttributes = array_merge($customAttributes, call_user_func(static::$customAttributesByUser, $User, $School));
        }

        return $customAttributes;
    }

    protected static function syncUserCustomAttributes(IPerson $User, array $lookerUser, LoggerInterface $logger = null, $pretend = true)
    {
        $userCustomAttributes = static::getUserCustomAttributes($User);
        $customAttributesToAdd = [];


        if ($UserMapping = static::getUserMapping($User)) {
            $currentUserCustomAttributes = LookerAPI::getUserCustomAttributes($UserMapping->ExternalIdentifier);
        } else {
            $currentUserCustomAttributes = [];
        }

        if (!empty($currentUserCustomAttributes)) {
            foreach ($currentUserCustomAttributes as $lookerCustomAttribute) {
                if (!isset($lookerCustomAttribute['name']) || !array_key_exists($lookerCustomAttribute['name'], $userCustomAttributes)) {
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
                'User Custom Attributes verified. ({remote})',
                [
                    'remote' => !empty($currentUserCustomAttributes) ? join(' | ', array_map(function($c) { return "$c[name]: $c[value]"; }, $currentUserCustomAttributes)) : []
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

            $logger->debug('Syncing user custom attributes for {slateEmail}: {attributesToSet}', [
                'slateEmail' => $User->Email,
                'attributesToSet' => join(', ', array_map(function($c) { return "$c[name] => $c[value]"; }, $customAttributesToAdd))
            ]);

            foreach ($customAttributesToAdd as $customAttributeId => $customAttributeToAdd) {
                if (!$pretend) {
                    $lookerResponse = LookerAPI::updateUserCustomAttribute($lookerUser['id'], $customAttributeId, $customAttributeToAdd);
                    if ($lookerResponse['value'] != $customAttributeToAdd['value']) {
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

    protected static function getUsersFromJob(IJob $Job)
    {
        $userConditions = [];
        if (!empty($Job->Config['pushSchools'])) {
            $userConditions['ID'] = [
                'values' => array_keys(
                    SchoolUser::getAllByWhere([
                        'SchoolID' => [
                            'values' => $Job->Config['pushSchools'],
                            'operator' => 'IN'
                        ]
                    ], [
                        'indexField' => 'PersonID'
                    ])
                ),
                'operator' => 'IN'
            ];
        }

        return User::getAllByWhere(array_merge([
            'AccountLevel' => [
                'value' => 'Disabled',
                'operator' => '!='
            ]
        ], $userConditions));
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
            'failed' => 0,
            'verified' => 0
        ];

        // get existing Looker users and index by email
        $LookerUsers = LookerAPI::getAllUsers(['fields' => 'id,first_name,last_name,email,group_ids,role_ids']);

        $LookerUsersMappedByEmail = [];
        for ($i = 0; $i < count($LookerUsers); $i++) {
            $LookerUser = $LookerUsers[$i];
            $LookerUsersMappedByEmail[$LookerUser['email']] = $LookerUser;
        }
        unset($LookerUsers);

        // iterate over Slate users
        $UsersToSync = static::getUsersFromJob($Job);

        foreach ($UsersToSync AS $User) {
            $Job->debug(
                'Analyzing Slate user {slateUsername} ({slateEmail})',
                [
                    'slateUsername' => $User->Username,
                    'slateEmail' => $User->Email
                ]
            );
            $results['analyzed']++;

            try {
                // create a user mapping if looker user exists with same email
                if (array_key_exists($User->Email, $LookerUsersMappedByEmail) && !static::getUserMapping($User)) {
                    static::createUserMapping($User, $LookerUser['id']);
                }

                $syncResult = static::pushUser($User, $Job, $pretend);

                if ($syncResult->getStatus() === SyncResult::STATUS_CREATED) {
                    $results['created']++;
                } elseif ($syncResult->getStatus() === SyncResult::STATUS_UPDATED) {
                    $results['updated']++;
                } elseif ($syncResult->getStatus() === SyncResult::STATUS_VERIFIED) {
                    $results['verified']++;
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

    /**
    * IdentityConsumer interface methods
    */
    public static function getSAMLNameId(IPerson $Person)
    {
        if (!$Person->Username) {
            throw new Exception('must have a username to connect to Looker');
        }

        return [
            'Format' => 'urn:oasis:names:tc:SAML:2.0:nameid-format:persistent',
            'Value' => $Person->Email
        ];
    }


    public static function getSAMLAttributes(IPerson $Person)
    {
        // TODO: add roles, groups, custom attributes?
        return [
            'Email' => [$Person->Email],
            'FName' => [$Person->FirstName],
            'LName' => [$Person->LastName]
            // removed until thoroughly tested
            //'Roles' => static::getUserRoles($Person)
        ];
    }
}
