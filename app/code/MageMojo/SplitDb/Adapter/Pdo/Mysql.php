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

class Mysql extends OriginalMysqlPdo
{
    const READ_ONLY_KEY = "readonly";
    const DEFAULT_DB_KEY = "default";

    const TRANSACTION = 64;

    private $exceptionMap;

    protected $_config = [];
    protected $_configRead = [];
    protected $_configWrite = [];
    private $_connectionRead;
    private $_connectionWrite;

    /**
     * Connect using conditions
     * @param bool $sql
     * @throws \Exception
     * @throws \Zend_Db_Profiler_Exception
     */
    protected function _connect($sql = false)
    {

        $isConnected = (bool) ($this->_connection !== null);

        if($isConnected && $this->isSelect($sql) && $this->isUsingReadConnection()){
            if($this->_connection !== $this->_connectionRead){
                $this->_connection = $this->_connectionRead;
            }
            return;
        }
        elseif($isConnected && !$this->isSelect($sql) && $this->isUsingWriteConnection()){
            if($this->_connection !== $this->_connectionWrite){
                $this->_connection = $this->_connectionWrite;
            }
            return;
        }

        //Get the connection according the sql query
        $this->getConnectionBySql($sql);

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
        $dsn = $this->_dsn();

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
        }

        try {

            $configDefault = $this->getConfig();
            $configRead = $this->getConfigRead();
            $configWrite = $this->getConfigWrite();

            $this->_connection = new \PDO(
                $dsn,
                $configDefault['username'],
                $configDefault['password'],
                $configDefault['driver_options']
            );
            $this->_connectionRead = new \PDO(
                $dsn,
                $configRead['username'],
                $configRead['password'],
                $configDefault['driver_options']
            );
            $this->_connectionWrite = new \PDO(
                $dsn,
                $configWrite['username'],
                $configWrite['password'],
                $configDefault['driver_options']
            );

            if($this->isSelect($sql)){
                $this->_connection = $this->_connectionRead;
            }else{
                $this->_connection = $this->_connectionWrite;
            }

            $this->_profiler->queryEnd($q);

            // set the PDO connection to perform case-folding on array keys, or not
            $this->_connection->setAttribute(\PDO::ATTR_CASE, $this->_caseFolding);
            $this->_connectionRead->setAttribute(\PDO::ATTR_CASE, $this->_caseFolding);
            $this->_connectionWrite->setAttribute(\PDO::ATTR_CASE, $this->_caseFolding);

            // always use exceptions.
            $this->_connection->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
            $this->_connectionRead->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
            $this->_connectionWrite->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        } catch (\PDOException $e) {
            throw new \Exception($e->getMessage(), $e->getCode(), $e);
        }

        $this->logger->logStats(LoggerInterface::TYPE_CONNECT, '');

        /** @link http://bugs.mysql.com/bug.php?id=18551 */
        $this->_connection->query("SET SQL_MODE=''");
        $this->_connectionRead->query("SET SQL_MODE=''");
        $this->_connectionWrite->query("SET SQL_MODE=''");

        // As we use default value CURRENT_TIMESTAMP for TIMESTAMP type columns we need to set GMT timezone
        $this->_connection->query("SET time_zone = '+00:00'");
        $this->_connectionRead->query("SET time_zone = '+00:00'");
        $this->_connectionWrite->query("SET time_zone = '+00:00'");

        if (isset($this->_config['initStatements'])) {
            $statements = $this->_splitMultiQuery($this->_config['initStatements']);
            foreach ($statements as $statement) {
                $this->_query($statement);
            }
        }

        if (!$this->_connectionFlagsSet) {
            $this->_connection->setAttribute(\PDO::ATTR_EMULATE_PREPARES, true);
            $this->_connectionRead->setAttribute(\PDO::ATTR_EMULATE_PREPARES, true);
            $this->_connectionWrite->setAttribute(\PDO::ATTR_EMULATE_PREPARES, true);
            if (isset($this->_config['use_buffered_query']) && $this->_config['use_buffered_query'] === false) {
                $this->_connection->setAttribute(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
                $this->_connectionRead->setAttribute(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
                $this->_connectionWrite->setAttribute(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
            } else {
                $this->_connection->setAttribute(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, true);
                $this->_connectionRead->setAttribute(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, true);
                $this->_connectionWrite->setAttribute(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, true);
            }
            $this->_connectionFlagsSet = true;
        }
    }

    /**
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
     * @return Mysql
     */
    private function getConnectionBySql($sql = 'write')
    {
        $db = ObjectManager::getInstance()->create(DeploymentConfig::class)->get('db');
        $connections = $db['connection'];

        $this->setConfigRead($connections[self::READ_ONLY_KEY]);
        $this->setConfigWrite($connections[self::DEFAULT_DB_KEY]);

        $isSelect = (bool) (($sql instanceof Select) || $this->isSelect($sql));
        $readConnectExists = (bool) array_key_exists(self::READ_ONLY_KEY, $connections);

        if (($isSelect && $readConnectExists) || $sql === 'read') {
            $this->setConfig($this->getConfigRead());
        }elseif ($sql === 'write'){
            $this->setConfig($this->getConfigWrite());
        }else{
            $this->setConfig($this->getConfigWrite());
        }

        return $this->getConfig();
    }

    /**
     * Validate SQL query
     * @param $sql
     * @return bool
     */
    private function isSelect(string $sql)
    {
        $availableTypes = ['select'];

        foreach ($availableTypes as $type) {
            if (strpos(strtolower($sql), $type) !== false) {
                return true;
            }
        }
        return false;
    }

    /**
     * Begin your Write Connection
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
        $this->_connectionWrite->beginTransaction();
    }

    /**
     * @throws \Exception
     */
    protected function _commit()
    {
        $this->_connect('write');
        $this->_connectionWrite->commit();
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
        $configRead = $this->getConfigRead();
        $configDefault = $this->getConfig();

        $isSameDb = (bool) ($configDefault['dbname'] === $configRead['dbname']);
        $isSameHost= (bool) ($configDefault['dbname'] === $configRead['dbname']);
        $isSamePassword = (bool) ($configDefault['dbname'] === $configRead['dbname']);
        $isSameUsername = (bool) ($configDefault['dbname'] === $configRead['dbname']);

        if($isSameDb && $isSameHost && $isSamePassword && $isSameUsername){
            return true;
        }
        return false;
    }

    /**
     * Check if it is using the Write Connection
     * @return bool
     */
    private function isUsingWriteConnection(){

        $configWrite = $this->getConfigWrite();
        $configDefault = $this->getConfig();

        $isSameDb = (bool) ($configDefault['dbname'] === $configWrite['dbname']);
        $isSameHost= (bool) ($configDefault['dbname'] === $configWrite['dbname']);
        $isSamePassword = (bool) ($configDefault['dbname'] === $configWrite['dbname']);
        $isSameUsername = (bool) ($configDefault['dbname'] === $configWrite['dbname']);

        if($isSameDb && $isSameHost && $isSamePassword && $isSameUsername){
            return true;
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
     * Insert in table
     * @param mixed $table
     * @param array $bind
     * @return int
     * @throws \Magento\Framework\Exception\LocalizedException
     * @throws \Zend_Db_Adapter_Exception
     * @throws \Zend_Db_Statement_Exception
     */
    public function insert($table, array $bind)
    {
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
     * @param mixed $table
     * @param string $where
     * @return int
     * @throws \Magento\Framework\Exception\LocalizedException
     * @throws \Zend_Db_Adapter_Exception
     * @throws \Zend_Db_Statement_Exception
     */
    public function delete($table, $where = '')
    {
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
     * @return mixed
     */
    public function getConfigRead()
    {
        return $this->_configRead;
    }

    /**
     * @param mixed $configRead
     * @return array
     */
    public function setConfigRead($configRead): array
    {
        $this->_configRead = $configRead;

        return $this->_configRead;
    }

    /**
     * @return mixed
     */
    public function getConfigWrite()
    {
        return $this->_configWrite;
    }

    /**
     * @param mixed $configWrite
     * @return array
     */
    public function setConfigWrite($configWrite): array
    {
        $this->_configWrite = $configWrite;

        return $this->_configWrite;
    }

    /**
     * @return mixed
     */
    public function getConfig()
    {
        return $this->_config;
    }

    /**
     * @param mixed $config
     * @return array
     */
    public function setConfig($config): array
    {
        $this->_config = $config;

        return $this->_config;
    }
}
