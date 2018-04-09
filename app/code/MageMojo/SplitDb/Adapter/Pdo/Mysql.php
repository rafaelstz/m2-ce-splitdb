<?php

namespace MageMojo\SplitDb\Adapter\Pdo;

use Magento\Framework\DB\Adapter\Pdo\Mysql as OriginalMysqlPdo;
use Magento\Framework\App\ObjectManager;
use Magento\Framework\DB\Adapter\AdapterInterface;
use Magento\Framework\DB\LoggerInterface;
use Magento\Framework\DB\Select;
use Magento\Framework\App\DeploymentConfig;
use Magento\Framework\DB\Adapter\ConnectionException;
use Magento\Framework\DB\Adapter\LockWaitException;
use Magento\Framework\DB\Adapter\DeadlockException;
use Magento\Framework\DB\Adapter\DuplicateException;
use Magento\Framework\DB\Profiler;
use Zend_Db_Select;

class Mysql extends OriginalMysqlPdo
{
    const READ_ONLY_KEY = "readonly";
    const DEFAULT_DB_KEY = "default";

    const TRANSACTION = 64;

    private $exceptionMap;

    protected $_config = [];
    protected $_configRead = [];
    protected $_configWrite = [];
    protected $_connection = [];
    protected $_connectionRead;
    protected $_connectionWrite;
    protected $_readConnectExists;

    /**
     * Connect using a query SQL or a string to write or read the database.
     * @param mixed $sql
     * @throws \Exception
     * @throws \Zend_Db_Profiler_Exception
     */

    protected function _connect($sql = false)
    {

        $isConnected = (bool) ($this->_connection);
        $toIgnore = $this->_ignoreSQL($sql);

        if($toIgnore && $this->isUsingWriteConnection()){
            $sql = 'write';
        }elseif($toIgnore && $this->isUsingReadConnection()){
            $sql = 'read';
        }

//        $this->_debug($sql);

        if($this->_transactionLevel !== 0){
            if($this->isUsingReadConnection()){
                $this->setConnection($this->_connectionWrite);
                $this->getConnectionBySql('write');
            }
            return;
        }

        // Check if the forced mode is the same currently utilized
        if($sql == 'write' && $this->isUsingReadConnection()){
            $this->closeConnection();
            $this->setConnection($this->_connectionWrite);
            $this->getConnectionBySql('write');
        }elseif($sql == 'read' && !$this->isUsingReadConnection()){
            $this->closeConnection();
            $this->setConnection($this->_connectionRead);
            $this->getConnectionBySql('read');
        }

        // Check if need to connect
        if($isConnected) {
            if ($this->isSelect($sql) && !$this->isUsingReadConnection()) {
                $this->setConnection($this->_connectionRead);
                $this->setConfig($this->getConfigRead());
                $this->getConnectionBySql('read');
            } elseif (!$this->isSelect($sql) && $this->isUsingReadConnection()) {
                $this->setConnection($this->_connectionWrite);
                $this->getConnectionBySql('write');
            } else {
                return;
            }
        }

        $this->getConnectionBySql($sql);


        if ($this->isSelect($sql) && !$this->isUsingReadConnection()) {
            $this->setConfig($this->getConfigRead());
            $this->setConnection($this->_connectionRead);
        } elseif (!$this->isSelect($sql) && $this->isUsingReadConnection()) {
            $this->setConfig($this->getConfigWrite());
            $this->setConnection($this->_connectionWrite);
        }


        if (!extension_loaded('pdo_mysql')) {
            throw new \Exception('pdo_mysql extension is not installed');
        }

        if (!isset($this->_config['host'])) {
            throw new \Exception('No host configured to connect');
        }

        if (isset($this->_config['port'])) {
            throw new \Exception('Port must be configured within host parameter (like localhost:3306');
        }

        unset($this->_config['port']);


        if (strpos($this->_config['host'], '/') !== false) {
            $this->_config['unix_socket'] = $this->_config['host'];
            unset($this->_config['host']);
        } elseif (strpos($this->_config['host'], ':') !== false) {
            list($this->_config['host'], $this->_config['port']) = explode(':', $this->_config['host']);
        }

        if (!isset($this->_config['driver_options'][\PDO::MYSQL_ATTR_MULTI_STATEMENTS])) {
            $this->_config['driver_options'][\PDO::MYSQL_ATTR_MULTI_STATEMENTS] = false;
        }

        $this->logger->startTimer();

        if (!empty($this->_config['charset'])
            && version_compare(PHP_VERSION, '5.3.6', '<')
        ) {
            $initCommand = "SET NAMES '" . $this->_config['charset'] . "'";
            $this->_config['driver_options'][1002] = $initCommand; // 1002 = PDO::MYSQL_ATTR_INIT_COMMAND
        }

        // get the dsn first, because some adapters alter the $_pdoType
        $dsn = $this->_dsn($sql);

        // check for PDO extension
        if (!extension_loaded('pdo')) {
            throw new \Exception(
                'The PDO extension is required for this adapter but the extension is not loaded'
            );
        }

        // check the PDO driver is available
        if (!in_array($this->_pdoType, \PDO::getAvailableDrivers())) {
            throw new \Exception('The ' . $this->_pdoType . ' driver is not currently installed');
        }

        // create PDO connection
        $q = $this->_profiler->queryStart('connect', \Zend_Db_Profiler::CONNECT);

        // add the persistence flag if we find it in our config array
        if (isset($this->_config['persistent']) && ($this->_config['persistent'] == true)) {
            $this->_config['driver_options'][\PDO::ATTR_PERSISTENT] = true;
            if($this->getReadConnectExists() == 2){
                $this->_configWrite['driver_options'][\PDO::ATTR_PERSISTENT] = true;
                $this->_configRead['driver_options'][\PDO::ATTR_PERSISTENT] = true;
            }
        }

        try {

            $configDefault = $this->getConfig();

            $this->_connection = new \PDO(
                $dsn,
                $configDefault['username'],
                $configDefault['password'],
                $configDefault['driver_options']
            );

            if($this->getReadConnectExists() == 2){

                $configRead = $this->getConfigRead();
                $configWrite = $this->getConfigWrite();

                $this->_connectionRead = new \PDO(
                    $this->_dsn('read'),
                    $configRead['username'],
                    $configRead['password'],
                    $configDefault['driver_options']
                );
                $this->_connectionWrite = new \PDO(
                    $this->_dsn('write'),
                    $configWrite['username'],
                    $configWrite['password'],
                    $configDefault['driver_options']
                );


                if($this->isSelect($sql)){
                    $this->_connection = $this->_connectionRead;
                }else{
                    $this->_connection = $this->_connectionWrite;
                }

            }

            $this->_profiler->queryEnd($q);

            // set the PDO connection to perform case-folding on array keys, or not
            $this->_connection->setAttribute(\PDO::ATTR_CASE, $this->_caseFolding);
            if($this->getReadConnectExists() == 2) {
                $this->_connectionRead->setAttribute(\PDO::ATTR_CASE, $this->_caseFolding);
                $this->_connectionWrite->setAttribute(\PDO::ATTR_CASE, $this->_caseFolding);
            }

            // always use exceptions.
            $this->_connection->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
            if($this->getReadConnectExists() == 2 ) {
                $this->_connectionRead->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
                $this->_connectionWrite->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
            }

        } catch (\PDOException $e) {
            throw new \Exception($e->getMessage(), $e->getCode(), $e);
        }

        $this->logger->logStats(LoggerInterface::TYPE_CONNECT, '');

        /** @link http://bugs.mysql.com/bug.php?id=18551 */
        $this->_connection->query("SET SQL_MODE=''");
        if($this->getReadConnectExists() == 2) {
            $this->_connectionRead->query("SET SQL_MODE=''");
            $this->_connectionWrite->query("SET SQL_MODE=''");
        }

        // As we use default value CURRENT_TIMESTAMP for TIMESTAMP type columns we need to set GMT timezone
        $this->_connection->query("SET time_zone = '+00:00'");
        if($this->getReadConnectExists() == 2) {
            $this->_connectionRead->query("SET time_zone = '+00:00'");
            $this->_connectionWrite->query("SET time_zone = '+00:00'");
        }

        if (isset($this->_config['initStatements'])) {
            $statements = $this->_splitMultiQuery($this->_config['initStatements']);
            foreach ($statements as $statement) {
                $this->_query($statement);
            }
        }

        if (!$this->_connectionFlagsSet) {
            $this->_connection->setAttribute(\PDO::ATTR_EMULATE_PREPARES, true);
            if($this->getReadConnectExists() == 2) {
                $this->_connectionRead->setAttribute(\PDO::ATTR_EMULATE_PREPARES, true);
                $this->_connectionWrite->setAttribute(\PDO::ATTR_EMULATE_PREPARES, true);
            }
            if (isset($this->_config['use_buffered_query']) && $this->_config['use_buffered_query'] === false) {
                $this->_connection->setAttribute(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
                if($this->getReadConnectExists() == 2) {
                    $this->_connectionRead->setAttribute(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
                    $this->_connectionWrite->setAttribute(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY,
                        false);
                }
            } else {
                $this->_connection->setAttribute(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, true);
                if($this->getReadConnectExists() == 2) {
                    $this->_connectionRead->setAttribute(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, true);
                    $this->_connectionWrite->setAttribute(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, true);
                }
            }
            $this->_connectionFlagsSet = true;
        }
    }

    protected function _debug($sql){
        if($sql !== false) {
            if ($this->isSelect($sql)) {
                if (is_string($sql)) {
                    var_dump($sql);
                } else {
                    $test = (string)$sql->assemble();
                    var_dump($test);
                }
                var_dump('using read');
            } else {
                if (is_string($sql)) {
                    var_dump($sql);
                }elseif (is_bool($sql)){
                    var_dump($sql);
                }
                var_dump('using write');
            }
        }
    }

    protected function _dsn($sql='write')
    {
        // baseline of DSN parts
        $dsn = $this->_config;

        $dsnCustomConfig = $this->isSelect($sql) ? $this->_configRead : $this->_configWrite;

        // don't pass the username, password, charset, persistent and driver_options in the DSN
        unset($dsnCustomConfig['username']);
        unset($dsnCustomConfig['password']);
        unset($dsn['options']);
        unset($dsn['charset']);
        unset($dsn['persistent']);
        unset($dsn['driver_options']);

        // use all remaining parts in the DSN
        foreach ($dsn as $key => $val) {
            $dsn[$key] = "$key=$val";
        }

        $dsn = $this->_pdoType . ':' . implode(';', $dsn);
        if (isset($this->_config['charset'])) {
            $dsn .= ';charset=' . $this->_config['charset'];
        }
        return $dsn;
    }

    /**
     * Update from Select using Write connection
     * @param Select $select
     * @param array|string $table
     * @return string
     * @throws \Exception
     */
    public function updateFromSelect(Select $select, $table){
        $this->_connect('write');
        return parent::updateFromSelect($select, $table);
    }


    /**
     * Load custom readonly connection on env.php
     * @param Select|string|boolean $sql
     * @return void
     * @throws \Zend_Db_Adapter_Exception
     * @throws \Zend_Db_Statement_Exception
     */
    private function getConnectionBySql($sql)
    {
        if($this->getReadConnectExists() == null) {
            $db = ObjectManager::getInstance()->create(DeploymentConfig::class)->get('db');
            $connections = $db['connection'];
            $readConnectExists = array_key_exists(self::READ_ONLY_KEY, $connections) ? 2 : 1;

            $this->setConfigRead($connections[self::READ_ONLY_KEY]);
            $this->setConfigWrite($connections[self::DEFAULT_DB_KEY]);

            $this->setReadConnectExists($readConnectExists);


        }elseif ($this->getReadConnectExists() == 2) {

            $isSelect = (bool)$this->isSelect($sql);

            if (($isSelect || $sql === 'read') && !$this->isUsingReadConnection()) {
                $this->closeConnection();
                $this->setConnection($this->_connectionRead);
            } elseif ((!$isSelect || $sql === 'write') && !$this->isUsingWriteConnection()) {
                $this->closeConnection();
                $this->setConnection($this->_connectionWrite);
            }
        }else{
            parent::_connect();
        }
    }

    /**
     * Check if the SQL query needs to be ignored or not
     * @param $sql
     * @return bool
     */
    protected function _ignoreSQL($sql){

        $ignoreTempQueries = ['eav_attribute', '_tmp_'];

        $toIgnore = ((strpos(substr($sql, 0, 10), 'SET NAMES') !== false) || $sql == false);

        if(!$toIgnore && $this->isSelect($sql)){
            $usingTempTable =  (isset($_SESSION['tempTables']) && $_SESSION['tempTables'] !== false);
            if($usingTempTable){
                foreach ($ignoreTempQueries as $query){
                    if(strpos($sql, $query) !== false){
//                        $this->_debug($sql);
                        $toIgnore = true;
                    };
                }
            }
        }

        return $toIgnore;
    }

    /**
     * Validate SQL query
     * @param $sql
     * @return bool
     */
    private function isSelect($sql)
    {

        $hasSelect = (bool) (
            (strpos(substr($sql, 0, 10), 'SELECT `') !== false)
//            || (strpos(substr($sql, 0, 10), 'SELECT @@') !== false)
        );
        $tmpTable = (bool)(
            (strpos($sql, 'tmp') !== false)
            || (strpos($sql, 'temp') !== false)
        );
        $toIgnore = (
            (strpos(substr($sql, 0, 10), 'SET NAMES') !== false)
            || $sql == false);
        $writeQueries = ['INSERT','UPDATE','DELETE','CREATE','DROP', 'RENAME', 'SELECT @@'];

        if($this->isUsingReadConnection() && $toIgnore) {
            return true;
        }elseif ($this->isUsingWriteConnection() && $toIgnore ){
            return false;
        }

        if($tmpTable){
            $_SESSION['tempTables'] = true;
            if(!$hasSelect) {
                return false;
            }
        }

        if ($sql instanceof Select) {
            $sql = (string) $sql->assemble();
        }

        if(is_string($sql)) {

            foreach ($writeQueries as $query) {
                if (strpos(strtoupper($sql), $query) !== false) {
                    return false;
                }
            }
            if ($hasSelect || $sql == 'read') {
                return true;
            }
        }


        return false;

    }

    /**
     * Begin the transaction using Write Connection
     * @return $this
     * @throws \Exception
     */
    public function beginTransaction()
    {

        if ($this->_isRolledBack) {
            throw new \Exception(AdapterInterface::ERROR_ROLLBACK_INCOMPLETE_MESSAGE);
        }
        if ($this->_transactionLevel === 0) {
            $this->logger->startTimer();
            $this->_connect('write');
            $q = $this->_profiler->queryStart('begin', self::TRANSACTION);
            $this->_beginTransaction();
            $this->_profiler->queryEnd($q);
            $this->logger->logStats(LoggerInterface::TYPE_TRANSACTION, 'BEGIN');
        }
        ++$this->_transactionLevel;
        return $this;
    }

    /**
     * Use commit method using Write Connection
     * @return $this
     * @throws \Exception
     */
    public function commit()
    {
        if ($this->_transactionLevel === 1 && !$this->_isRolledBack) {
            $this->logger->startTimer();
            $this->_connect('write');
            $q = $this->_profiler->queryStart('commit', self::TRANSACTION);
            $this->_commit();
            $this->_profiler->queryEnd($q);
            $this->logger->logStats(LoggerInterface::TYPE_TRANSACTION, 'COMMIT');
        } elseif ($this->_transactionLevel === 0) {
            throw new \Exception(AdapterInterface::ERROR_ASYMMETRIC_COMMIT_MESSAGE);
        } elseif ($this->_isRolledBack) {
            throw new \Exception(AdapterInterface::ERROR_ROLLBACK_INCOMPLETE_MESSAGE);
        }
        --$this->_transactionLevel;
        return $this;
    }

    /**
     * Rollback using Write Connection
     * @return $this
     * @throws \Exception
     */
    public function rollBack()
    {
        if ($this->_transactionLevel === 1) {
            $this->logger->startTimer();
            $this->_connect('write');
            $q = $this->_profiler->queryStart('rollback', self::TRANSACTION);
            $this->_rollBack();
            $this->_profiler->queryEnd($q);
            $this->_isRolledBack = false;
            $this->logger->logStats(LoggerInterface::TYPE_TRANSACTION, 'ROLLBACK');
        } elseif ($this->_transactionLevel === 0) {
            throw new \Exception(AdapterInterface::ERROR_ASYMMETRIC_ROLLBACK_MESSAGE);
        } else {
            $this->_isRolledBack = true;
        }
        --$this->_transactionLevel;
        return $this;
    }

    /**
     * @throws \Exception
     */
    protected function _beginTransaction()
    {
        $this->_connect('write');
        $this->_connection->beginTransaction();
    }

    /**
     * @throws \Exception
     */
    protected function _commit()
    {
        $this->_connect('write');
        $this->_connection->commit();
    }

    /**
     * @throws \Exception
     */
    protected function _rollBack() {
        $this->_connect('write');
        $this->_connectionWrite->rollBack();
    }

    /**
     * Rewrite original mysql pdo to use another database in select queries
     * @param Select|string $sql
     * @param array $bind
     * @return \PDOStatement
     * @throws \Exception
     */
    protected function _query($sql, $bind = [])
    {

        $this->exceptionMap = [
            // SQLSTATE[HY000]: General error: 2006 MySQL server has gone away
            2006 => ConnectionException::class,
            // SQLSTATE[HY000]: General error: 2013 Lost connection to MySQL server during query
            2013 => ConnectionException::class,
            // SQLSTATE[HY000]: General error: 1205 Lock wait timeout exceeded
            1205 => LockWaitException::class,
            // SQLSTATE[40001]: Serialization failure: 1213 Deadlock found when trying to get lock
            1213 => DeadlockException::class,
            // SQLSTATE[23000]: Integrity constraint violation: 1062 Duplicate entry
            1062 => DuplicateException::class,
        ];

        $this->getConnectionBySql($sql);

        $connectionErrors = [
            2006, // SQLSTATE[HY000]: General error: 2006 MySQL server has gone away
            2013,  // SQLSTATE[HY000]: General error: 2013 Lost connection to MySQL server during query
        ];
        $triesCount = 0;
        do {
            $retry = false;
            $this->logger->startTimer();
            try {
                $this->_checkDdlTransaction($sql);
                $this->_prepareQuery($sql, $bind);
                $result = $this->pdoAbstractQuery($sql, $bind);
                $this->logger->logStats(LoggerInterface::TYPE_QUERY, $sql, $bind, $result);
                return $result;
            } catch (\Exception $e) {
                // Finalize broken query
                $profiler = $this->getProfiler();
                if ($profiler instanceof Profiler) {
                    /** @var Profiler $profiler */
                    $profiler->queryEndLast();
                }

                /** @var $pdoException \PDOException */
                $pdoException = null;
                if ($e instanceof \PDOException) {
                    $pdoException = $e;
                } elseif (($e instanceof \Exception)
                    && ($e->getPrevious() instanceof \PDOException)
                ) {
                    $pdoException = $e->getPrevious();
                }

                // Check to reconnect
                if ($pdoException && $triesCount < self::MAX_CONNECTION_RETRIES
                    && in_array($pdoException->errorInfo[1], $connectionErrors)
                ) {
                    $retry = true;
                    $triesCount++;
                    $this->closeConnection();
                    $this->_connect($sql);
                }

                if (!$retry) {
                    $this->logger->logStats(LoggerInterface::TYPE_QUERY, $sql, $bind);
                    $this->logger->critical($e);
                    // rethrow custom exception if needed
                    if ($pdoException && isset($this->exceptionMap[$pdoException->errorInfo[1]])) {
                        $customExceptionClass = $this->exceptionMap[$pdoException->errorInfo[1]];
                        /** @var \Zend_Db_Adapter_Exception $customException */
                        $customException = new $customExceptionClass($e->getMessage(), $pdoException->errorInfo[1], $e);
                        throw $customException;
                    }
                    throw $e;
                }
            }
        } while ($retry);
    }

    /**
     * Check if it is using the Read Connection
     * @return bool
     */
    private function isUsingReadConnection(){

        if($this->getReadConnectExists() == 2){

            $configRead = $this->getConfigRead();
            $configDefault = $this->getConfig();

            if(count($configDefault) > 0) {
                $isSameHost = (bool)($configDefault['host'] === $configRead['host']);
                $isSameDb = (bool)($configDefault['dbname'] === $configRead['dbname']);
                $isSamePassword = (bool)($configDefault['password'] === $configRead['password']);
                $isSameUsername = (bool)($configDefault['username'] === $configRead['username']);

                if($isSameDb && $isSameHost && $isSamePassword && $isSameUsername){
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Check if it is using the Write Connection
     * @return bool
     */
    private function isUsingWriteConnection(){
        if($this->getReadConnectExists() == 2) {
            $configWrite = $this->getConfigWrite();
            $configDefault = $this->getConfig();

            if(count($configDefault) > 0) {
                $isSameHost = (bool)($configDefault['host'] === $configWrite['host']);
                $isSameDb = (bool)($configDefault['dbname'] === $configWrite['dbname']);
                $isSamePassword = (bool)($configDefault['password'] === $configWrite['password']);
                $isSameUsername = (bool)($configDefault['username'] === $configWrite['username']);

                if ($isSameDb && $isSameHost && $isSamePassword && $isSameUsername) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Method magento/zendframework1/library/Zend/Db/Adapter/Abstract
     * @param $sql
     * @param array $bind
     * @return \PDOStatement
     * @throws \Exception
     */
    private function abstractAdapterQuery($sql, $bind = array())
    {
        $this->_connect($sql);

        if ($sql instanceof \Zend_Db_Select) {
            if (empty($bind)) {
                $bind = $sql->getBind();
            }

            $sql = $sql->assemble();
        }

        if (!is_array($bind)) {
            $bind = array($bind);
        }

        $stmt = $this->prepare($sql);
        $stmt->execute($bind);

        $stmt->setFetchMode($this->_fetchMode);
        return $stmt;
    }

    /**
     * Method from magento/framework/DB/Adapter/Pdo/Mysql
     * @param $sql
     * @param array $bind
     * @return \PDOStatement
     * @throws \Exception
     */
    private function pdoAbstractQuery($sql, $bind = array())
    {


        $this->_connect($sql);

        if (empty($bind) && $sql instanceof \Zend_Db_Select) {
            $bind = $sql->getBind();
        }

        if (is_array($bind)) {
            foreach ($bind as $name => $value) {
                if (!is_int($name) && !preg_match('/^:/', $name)) {
                    $newName = ":$name";
                    unset($bind[$name]);
                    $bind[$newName] = $value;
                }
            }
        }

        try {
            return $this->abstractAdapterQuery($sql, $bind);
        } catch (\PDOException $e) {
            /**
             * @see Zend_Db_Statement_Exception
             */
            throw new \Zend_Db_Statement_Exception($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * Prepares and executes an SQL statement with bound data.
     *
     * @param  mixed $sql The SQL statement with placeholders.
     *                      May be a string or Zend_Db_Select.
     * @param  mixed $bind An array of data to bind to the placeholders.
     * @return \PDOStatement
     * @throws \Exception
     * @throws \Zend_Db_Profiler_Exception
     */
    public function query($sql, $bind = array())
    {
        $this->_connect($sql);

        // is the $sql a Zend_Db_Select object?
        if ($sql instanceof Zend_Db_Select) {
            if (empty($bind)) {
                $bind = $sql->getBind();
            }

            $sql = $sql->assemble();
        }

        // make sure $bind to an array;
        // don't use (array) typecasting because
        // because $bind may be a Zend_Db_Expr object
        if (!is_array($bind)) {
            $bind = array($bind);
        }

        // prepare and execute the statement with profiling
        $stmt = $this->prepare($sql);
        $stmt->execute($bind);

        // return the results embedded in the prepared statement object
        $stmt->setFetchMode($this->_fetchMode);
        return $stmt;
    }

    /**
     * Insert in table
     * @param mixed $table
     * @param array $bind
     * @return int
     * @throws \Exception
     * @throws \Zend_Db_Adapter_Exception
     * @throws \Zend_Db_Profiler_Exception
     */
    public function insert($table, array $bind)
    {
        $this->_connect('write');

        $cols = array();
        $vals = array();
        $i = 0;
        foreach ($bind as $col => $val) {
            $cols[] = $this->quoteIdentifier($col, true);
            if ($val instanceof \Zend_Db_Expr) {
                $vals[] = $val->__toString();
                unset($bind[$col]);
            } else {
                if ($this->supportsParameters('positional')) {
                    $vals[] = '?';
                } else {
                    if ($this->supportsParameters('named')) {
                        unset($bind[$col]);
                        $bind[':col'.$i] = $val;
                        $vals[] = ':col'.$i;
                        $i++;
                    } else {
                        /** @see Zend_Db_Adapter_Exception */
                        #require_once 'Zend/Db/Adapter/Exception.php';
                        throw new \Zend_Db_Adapter_Exception(get_class($this) ." doesn't support positional or named binding");
                    }
                }
            }
        }

        // build the statement
        $sql = "INSERT INTO "
            . $this->quoteIdentifier($table, true)
            . ' (' . implode(', ', $cols) . ') '
            . 'VALUES (' . implode(', ', $vals) . ')';

        // execute the statement and return the number of affected rows
        if ($this->supportsParameters('positional')) {
            $bind = array_values($bind);
        }

        $stmt = $this->query($sql, $bind);
        $result = $stmt->rowCount();
        return $result;
    }


    /**
     * Execute a Delete query
     * @param mixed $table
     * @param string $where
     * @return int
     * @throws \Exception
     * @throws \Zend_Db_Profiler_Exception
     */
    public function delete($table, $where = '')
    {
        $this->getConnectionBySql('write');

        $where = $this->_whereExpr($where);

        /**
         * Build the DELETE statement
         */
        $sql = "DELETE FROM "
            . $this->quoteIdentifier($table, true)
            . (($where) ? " WHERE $where" : '');

        /**
         * Execute the statement and return the number of affected rows
         */
        $stmt = $this->query($sql);
        $result = $stmt->rowCount();
        return $result;
    }

    /**
     * Get the configuration read the database
     * @return mixed
     */
    public function getConfigRead()
    {
        return $this->_configRead;
    }

    /**
     * Set configuration just to read the database
     * @param mixed $configRead
     * @return array
     */
    public function setConfigRead($configRead)
    {
        $this->_configRead = $configRead;

        return $this->_configRead;
    }

    /**
     * Get the configuration write and read the database
     * @return mixed
     */
    public function getConfigWrite()
    {
        return $this->_configWrite;
    }

    /**
     * Set configuration to write/read the database
     * @param mixed $configWrite
     * @return array
     */
    public function setConfigWrite($configWrite)
    {
        $this->_configWrite = $configWrite;

        return $this->_configWrite;
    }

    /**
     * Get the current config to write or / and read the database
     * @return mixed
     */
    public function getConfig()
    {
        return $this->_config;
    }

    /**
     * Set the default configuration to use the database
     * @param mixed $config
     * @return array
     */
    public function setConfig($config)
    {
        $this->_config = $config;

        return $this->_config;
    }

    /**
     * Check if the secondary connection exists
     * @return mixed
     */
    public function getReadConnectExists()
    {
        return $this->_readConnectExists;
    }

    /**
     * Set the secondary connection status
     * @param bool
     * @return void
     */
    public function setReadConnectExists($readConnectExists)
    {
        $this->_readConnectExists = $readConnectExists;
    }

    /**
     * @return array
     */
    public function getConnection(){
        return $this->_connection;
    }

    /**
     * @param array $connection
     */
    public function setConnection($connection){
        $this->_connection = $connection;
    }
}
