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

    public static function login()
    {
        if (!static::$clientId) {
            throw new \Exception('Looker API clientId must be configured.');
        }

        if (!static::$clientSecret) {
            throw new \Exception('Looker API clientSecret must be configured.');
        }

        $response = static::post('login', [
            'client_id' => static::$clientId,
            'client_secret' => static::$clientSecret
        ], [
            'skipAuth' => true
        ]);

        if (isset($response['access_token'])) {
            static::$accessToken = $response['access_token'];
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
        \MICS::dump($data, 'createUser');
        $user = static::post('users', $data);
        \MICS::dump($user, 'created user');
        return $user;
    }

    public static function patchUser($userId, $data = [])
    {
        \MICS::dump($data, $userId, true);
        return static::post('users/'.$userId, $data);
    }
    // https://building21.looker.com:19999/api-docs/index.html#!/3.1/User/set_user_roles
    public static function updateUserRoles($userId, $data = [])
    {
        return static::put('users/'.$userId.'/roles', $data);
    }

    public static function updateUserGroups($userId, $data = [])
    {
        return static::put('users/'.$userId.'/groups', $data);
    }

    public static function get($endpoint, $options = [])
    {
        return static::request($endpoint, $options);
    }

    public static function post($endpoint, $data = [], $options = [])
    {
        return static::request($endpoint, array_merge([
            'post' => $data
        ], $options));
    }

    public static function put($endpoint, $data = [], $options = [])
    {
        return static::request($endpoint, array_merge([
            'method' => 'PUT',
            'post' => $data
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
            $encodeData = $options['post']['encode'];
            unset($options['post']['encode']);

            $postData = !empty($encodeData) ? json_encode($options['post']) : $options['post'];
            if (empty($options['method']) || $options['method'] == 'POST') {
                curl_setopt($ch, CURLOPT_POST, true);
            } else {
                curl_setopt($ch, CURLOPT_CUSTOMREQUEST, $options['method']);
            }

            curl_setopt($ch, CURLOPT_POSTFIELDS, $postData);
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