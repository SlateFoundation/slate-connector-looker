<?php

namespace Slate\Connectors\Looker;

use Psr\Log\LogLevel;
use Psr\Log\LoggerInterface;

use Slate;
use Slate\People\Student;

use Looker\API AS LookerAPI;

use Emergence\Connectors\AbstractConnector;
use Emergence\Connectors\ISynchronize;
use Emergence\Connectors\Job;
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

    public static $rolesByAccountLevel = [];
    public static $rolesByAccountType = [
        Student::class => [],
        User::class => []
    ];

    // workflow implementations
    protected static function _getJobConfig(array $requestData)
    {
        $config = parent::_getJobConfig($requestData);

        $config['clientId'] = $requestData['clientId'];
        $config['clientSecret'] = $requestData['clientSecret'];
        $config['pushUsers'] = !empty($requestData['pushUsers']);

        return $config;
    }

    public static function synchronize(Job $Job, $pretend = true)
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
        LookerAPI::$clientId = $Job->Config['clientId'];
        LookerAPI::$clientSecret = $Job->Config['clientSecret'];


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

            // sync groups
            try {
                $groupSyncResult = static::syncUserGroups($User, $lookerUser, $logger, $pretend);
            } catch (SyncException $e) {
                $logger->error(
                    $e->getInterpolatedMessage(),
                    $e->getContext()
                );
            }
            // sync roles
            try {
                $rolesSyncResult = static::syncUserRoles($User, $lookerUser, $logger, $pretend);
            } catch (SyncException $e) {
                $logger->error(
                    $e->getInterpolatedMessage(),
                    $e->getContext()
                );
            }
            // static::syncUserCustomAttributes($User, $lookerUser);

            // check for custom attributes


            return new SyncResult(
                !empty($changes) ? SyncResult::STATUS_UPDATED : SyncResult::STATUS_VERIFIED,
                'Looker account for {slateUsername} found and verified up-to-date.',
                [
                    'slateUsername' => $User->Username
                ]
            );
        } else { // trly to create user if no mapping found
            // skip accounts with no email
            if (!$User->PrimaryEmail) {
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

            // static::syncUserCustomAttributes($User, $lookerUser);

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
        if (isset(static::$groupsByAccountLevel[$User->AccountLevel])) {
            $roleIds = array_merge($roleIds, static::$rolesByAccountLevel[$User->AccountLevel]);
        }

        if (isset(static::$groupsByAccountType[$User->Class])) {
            $roleIds = array_merge($roleIds, static::$rolesByAccountType[$User->Class]);
        }
        return $roleIds;
    }

    protected static function syncUserRoles(IPerson $User, array $lookerUser, LoggerInterface $logger = null, $pretend = true)
    {
        $roleIds = $lookerUser['role_ids'];
        $userRoles = static::getUserRoles($User);

        $logger->debug(
            'Analyzing user roles: [{roleIds}]',
            [
                'roleIds' => join(',', $roleIds)
            ]
        );

        if ($rolesToAdd = array_diff($userRoles, $roleIds)) {
            $logger->notice(
                'Updating {slateUsername} roles to: [{userRoles}]',
                [
                    'slateUsername' => $User->Username,
                    'userRoles' => join(',', array_unique(array_merge($userRoles, $roleIds)))
                ]
            );

            if (!$pretend) {
                $lookerResponse = LookerAPI::updateUserRoles($lookerUser['id'], $rolesToAdd);

                if (empty($lookerResponse['role_ids']) || array_diff($lookerResponse['role_ids'], $rolesToAdd)) {
                    return new SyncException(
                        'Unable to sync user roles.',
                        [
                            'lookerResponse' => $lookerResponse
                        ]
                    );
                }

                $logger->notice(
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
                    'userRoles' => $roleIds
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
        return $groupIds;
    }

    protected static function syncUserGroups(IPerson $User, array $lookerUser, LoggerInterface $logger = null, $pretend = true)
    {
        $groupIds = $lookerUser['group_ids'];
        $userGroups = array_values(static::getUserGroups($User));
        $logger->debug(
            'Analyzing user groups: [{groupIds}]',
            [
                'groupIds' => join(',', $groupIds)
            ]
        );

        if ($groupsToAdd = array_diff($userGroups, $groupIds)) {
            $logger->notice(
                'Updating {slateUsername} groups to: [{userGroups}]',
                [
                    'slateUsername' => $User->Username,
                    'userGroups' => join(',', array_unique(array_merge($userGroups, $groupIds)))
                ]
            );

            // sync user groups via API
            foreach ($groupsToAdd as $groupId) {
                if ($pretend) {
                    $logger->notice(
                        'Adding user to group with ID: {groupId}',
                        [
                            'groupId' => $groupId
                        ]
                    );
                    continue;
                }

                $lookerResponse = LookerAPI::addUserToGroup($lookerUser['id'], $groupId);

                if (!empty($lookerResponse['group_ids']) && in_array($groupId, $lookerResponse['group_ids'])) {
                    $logger->notice(
                        'Ådded user to group with ID: {groupId}',
                        [
                            'lookerResponse' => $lookerResponse,
                            'groupId' => $groupId
                        ]
                    );
                } else {
                    $logger->notice(
                        'Error adding user to group with ID: {groupId}',
                        [
                            'groupId' => $groupId,
                            'lookerResponse' => $lookerResponse
                        ]
                    );
                }
            }

            return new SyncResult(
                empty($groupsToAdd) ? SyncResult::STATUS_VERIFIED : SyncResult::STATUS_UPDATED,
                'Updated groups for {slateUsername} in Looker ' . $pretend ? '(pretend-mode)' : '',
                [
                    'slateUsername' => $User->Username
                ]
            );
        }
    }

    protected static function syncUserCustomAttributes(IPerson $User, LoggerInterface $logger = null, $customAttributes = [], $pretend = true)
    {

    }

    // task handlers
    public static function pushUsers(Job $Job, $pretend = true)
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