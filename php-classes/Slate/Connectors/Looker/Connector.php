<?php

namespace Slate\Connectors\Looker;

use Psr\Log\LogLevel;
use Psr\Log\LoggerInterface;

use Slate;

use Looker\API AS LookerAPI;

use Emergence\Connectors\AbstractConnector;
use Emergence\Connectors\ISynchronize;
use Emergence\Connectors\Job;
use Emergence\Connectors\Mapping;
use Emergence\Connectors\Exceptions\SyncException;
use Emergence\Connectors\SyncResult;

use Emergence\People\User;
use Emergence\People\ContactPoint\Email AS EmailContactPoint;
use Emergence\Util\Data AS DataUtil;

class Connector extends AbstractConnector implements ISynchronize
{
    public static $title = 'Looker';
    public static $connectorId = 'looker';

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

        if (empty($Job->Config['clientSec'])) {
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

            if ($lookerUser['first_name'] != $User->PreferredName ?: $User->FirstName) {
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

            // if (array_diff($lookerUser['group_ids'], array_keys(static::getUserGroups()))) {
            //     // add missing group ids
            // }

            // if (array_diff($lookerUser['group_ids'], array_keys(static::getUserGroups()))) {
            //     // add missing group ids
            // }

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

            if ($pretend) {
                $logger->notice(
                    'Created Looker user for {slateUsername}',
                    [
                        'slateUsername' => $User->Username
                    ]
                );

                return new SyncResult(
                    SyncResult::STATUS_CREATED,
                    'Created Looker user for {slateUsername}, saved mapping to new Looker user (pretend-mode)',
                    [
                        'slateUsername' => $User->Username
                    ]
                );
            }

            $lookerResponse = LookerAPI::createUser([
                'first_name' => $User->FirstName,
                'last_name' => $User->LastName,
                'email' => $User->PrimaryEmail
            ]);

            $logger->notice(
                'Created Looker user for {slateUsername}',
                [
                    'slateUsername' => $User->Username,
                    'lookerResponse' => $lookerResponse
                ]
            );

            if (!empty($lookerResponse['id'])) {
                $mappingData['ExternalIdentifier'] = $lookerResponse['id'];
                Mapping::create($mappingData, true);

                return new SyncResult(
                    SyncResult::STATUS_CREATED,
                    'Created Looker user for {slateUsername}, saved mapping to new Looker user #{lookerUserId}',
                    [
                        'slateUsername' => $User->Username,
                        'lookerUserId' => $lookerResponse['id']
                    ]
                );
            } else {
                throw new SyncException(
                    'Failed to create Looker user for {slateUsername}',
                    [
                        'slateUsername' => $User->Username,
                        'lookerResponse' => $lookerResponse
                    ]
                );
            }
        }
    }

    protected static function syncUserRoles(IPerson $User, LoggerInterface $logger = null, $roleIds = [], $pretend = true)
    {

    }

    protected static function syncUserGroups(IPerson $User, LoggerInterface $logger = null, $groupIds = [], $pretend = true)
    {

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