<?php

namespace Looker;

class API
{
    public static $baseUrl = 'https://building21.looker.com';
    public static $apiRoot = '/api/3.1/';
    public static $portNumber = 19999;

    public static $clientId;
    public static $clientSecret;
    protected static $accessToken;

    public static function login($clientId = null, $clientSecret = null)
    {
        $clientId = $clientId ?: static::$clientId;
        $clientSecret = $clientSecret ?: static::$clientSecret;

        if (!$clientId) {
            throw new \Exception('Looker API clientId must be configured.');
        }

        if (!$clientSecret) {
            throw new \Exception('Looker API clientSecret must be configured.');
        }

        $response = static::request('login', [
            'post' => [
                'client_id' => $clientId,
                'client_secret' => $clientSecret,
                'encode' => false
            ],
            'skipAuth' => true
        ]);

        if (isset($response['access_token'])) {
            static::$accessToken = $response['access_token'];
        } else {
            throw new \Exception('Unable to retrieve access_token from Looker.');
        }
    }

    // TODO: loop through and get all users?
    public static function getAllUsers($params = [], $options = [])
    {
        $users = static::get('users', array_merge([
            'get' => array_merge([
                'per_page' => 100
            ], $params)
        ], $options));

        return $users;
    }

    public static function getUserById($id)
    {
        return static::get('users/'.$id);
    }

    #    https://building21.looker.com:19999/api-docs/index.html#!/3.1/User/search_users
    public static function searchUser($params = [])
    {
        return static::get('users/search', $params);
    }

    public static function findUserByEmail(string $email)
    {
        $user = static::searchUser([
            'email' => $email
        ]);

        if (count($user)) {
            return $user[0];
        }

        return null;
    }

    public static function createUser(array $data)
    {
        return static::post('users', $data);
    }

    public static function updateUser($userId, $data = [])
    {
        \MICS::dump($data, $userId);
        return static::post('users/'.$userId, $data);
    }
    // https://building21.looker.com:19999/api-docs/index.html#!/3.1/User/set_user_roles
    public static function updateUserRoles($userId, $data = [])
    {
        return static::put('users/'.$userId.'/roles', $data);
    }

    public static function addUserToGroup($userId, $groupId)
    {
        $endpoint = 'groups/';
        $endpoint .= $groupId;
        $endpoint .= '/users';

        return static::post($endpoint, [
            'user_id' => $userId
        ]);
    }

    public static function getUserCustomAttributes($userId)
    {
        return static::get('users/'.$userId.'/attribute_values');
    }

    public static function updateUserCustomAttribute($userId, $attributeId, $attributeData)
    {
        return static::patch('users/'.$userId.'/attribute_values/'.$attributeId, $attributeData);
    }

    public static function get($endpoint, $options = [])
    {
        return static::request($endpoint, $options);
    }

    public static function post($endpoint, $data = [], $options = [])
    {
        $requestOptions = array_merge_recursive([
            'post' => array_merge([
                'encode' => true
            ], $data)
        ], $options);

        return static::request($endpoint, $requestOptions);
    }

    public static function put($endpoint, $data = [], $options = [])
    {
        return static::request($endpoint, array_merge([
            'method' => 'PUT',
            'post' => array_merge(['encode' => true], $data)
        ], $options));
    }

    public static function patch($endpoint, $data = [], $options = [])
    {
        return static::request($endpoint, array_merge([
            'method' => 'PATCH',
            'post' => array_merge(['encode' => true], $data)
        ], $options));
    }

    public static function request($path, array $options = [])
    {
        if (!static::$accessToken && $path != 'login') {
            static::login();
        }

        // init get params
        if (empty($options['get'])) {
            $options['get'] = [];
        }
        // init post params
        if (empty($options['post'])) {
            $options['post'] = [];
        }
        // init headers
        if (empty($options['headers'])) {
            $options['headers'] = [];
        }

        $options['headers'][] = 'User-Agent: emergence';

        // init url
        if (preg_match('/^https?:\/\//', $path)) {
            $url = $path;
        } else {
            $url  = static::$baseUrl;
            $url .= ':'.static::$portNumber;
            $url .= static::$apiRoot;
            $url .= $path;
        }

        if (!empty($options['get'])) {
            $url .= '?' . http_build_query(array_map(function($value) {
                if (is_bool($value)) {
                    return $value ? 'true' : 'false';
                }
                return $value;
            }, $options['get']));
        }

        // configure curl
        $ch = curl_init($url);

        // configure encoding
        $encoding = !empty($options['encoding']) ? $options['encoding'] : 'UTF-8';
        curl_setopt($ch, CURLOPT_ENCODING, $encoding);

        // configure auth
        if (empty($options['skipAuth'])) {
            $options['headers'][] = 'Authorization: token '. static::$accessToken;
        }

        // configure output
        if (!empty($options['outputPath'])) {
            $fp = fopen($options['outputPath'], 'w');
            curl_setopt($ch, CURLOPT_FILE, $fp);
        } else {
            curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        }

        // configure method and body
        if (!empty($options['post'])) {
            $encode = !empty($options['post']['encode']);
            unset($options['post']['encode']);

            if (empty($options['method']) || $options['method'] == 'POST') {
                curl_setopt($ch, CURLOPT_POST, true);
            } else {
                curl_setopt($ch, CURLOPT_CUSTOMREQUEST, $options['method']);
            }
            curl_setopt($ch, CURLOPT_POSTFIELDS, $encode ? json_encode($options['post']) : $options['post']);
        }

        // configure headers
        curl_setopt($ch, CURLOPT_HTTPHEADER, $options['headers']);
        // execute request
        $result = curl_exec($ch);

        if (isset($fp)) {
            fclose($fp);
        } elseif (!isset($options['decodeJson']) || $options['decodeJson']) {
            $result = json_decode($result, true);
        }

        $httpStatus = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        curl_close($ch);

        if ($httpStatus !== 200) {
            \Emergence\Logger::general_error('Looker API Error', [
                'exceptionClass' => static::class,
                'exceptionMessage' => $result,
                'exceptionCode' => $httpStatus
            ]);
        }

        return $result;
    }
}