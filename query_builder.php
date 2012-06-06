<?php

/**
 * Class for building queries
 *
 * @package		Controller
 * @license		AGPL1
 * @version		0.1.1
 * @author		Roberto Pérez Nygaard - roberto@nygaard.es
 */
class QueryBuilder
{
	//Constants
	const COLUMNS = 'columns';
	const FROM = 'from';
	const JOIN = 'join';
	const UNION = 'union';
	const WHERE = 'where';
	const GROUP = 'group';
	const HAVING = 'having';
	const ORDER = 'order';
	const VALUES = 'values';
	const INTO = 'into';
	const LIMIT_COUNT = 'limitcount';
	const LIMIT_OFFSET = 'limitoffset';
	const FOR_UPDATE = 'forupdate';
	const INNER_JOIN = 'inner join';
	const LEFT_JOIN = 'left join';
	const RIGHT_JOIN = 'right join';
	const FULL_JOIN = 'full join';
	const CROSS_JOIN = 'cross join';
	const NATURAL_JOIN = 'natural join';

	const SQL_WILDCARD = '*';
	const SQL_SELECT = 'SELECT';
	const SQL_INSERT = 'INSERT';
	const SQL_UPDATE = 'UPDATE';
	const SQL_DELETE = 'DELETE';
	const SQL_UNION = 'UNION';
	const SQL_UNION_ALL = 'UNION ALL';
	const SQL_FROM = 'FROM';
	const SQL_JOIN = 'INNER JOIN';
	const SQL_INNER_JOIN = 'INNER JOIN';
	const SQL_LEFT_JOIN = 'LEFT JOIN';
	const SQL_WHERE = 'WHERE';
	const SQL_DISTINCT = 'DISTINCT';
	const SQL_GROUP_BY = 'GROUP BY';
	const SQL_ORDER_BY = 'ORDER BY';
	const SQL_HAVING = 'HAVING';
	const SQL_LIMIT = 'LIMIT';
	const SQL_FOR_UPDATE = 'FOR UPDATE';
	const SQL_AND = 'AND';
	const SQL_AS = 'AS';
	const SQL_SET = 'SET';
	const SQL_OR = 'OR';
	const SQL_ON = 'ON';
	const SQL_ASC = 'ASC';
	const SQL_DESC = 'DESC';
	const SQL_INTO = 'INTO';
	const SQL_VALUES = 'VALUES';
	const SQL_BEGIN = 'BEGIN';
	const SQL_COMMIT = 'COMMIT';
	const SQL_ROLLBACK = 'ROLLBACK';
	const SQL_QUERY_DIVISOR = ';';

	const CARRY_RETURN = "\n";

	//private variables
	private $_whereAmbit = 0;
	private $_partsBuffer = array();
	private $_lastQuery;

	//Protected variables
	protected $_adapter;

	/**
	 * Specify legal join types.
	 *
	 * @var array
	 */
	protected static $_joinSQL = array(
		self::FROM => self::SQL_FROM,
		self::JOIN => self::SQL_JOIN,
		self::INNER_JOIN => self::SQL_INNER_JOIN,
		self::LEFT_JOIN => self::SQL_LEFT_JOIN
	);

	/**
	 * Specify legal union types.
	 *
	 * @var array
	 */
	protected static $_unionTypes = array(
		self::SQL_UNION,
		self::SQL_UNION_ALL
	);


	protected $_parts = array();

	/**
	 * Class constructor
	 */
	public function __construct($adapter = null)
	{
		if (!$adapter)
		{
			$this->_exception('Empty adapter passed to ' . __CLASS__);
		}
		$this->_adapter = $adapter;
	}

	/**
	 * Forces a Variable to be as an Array
	 *
	 * @param mixed $var
	 * @param mixed $inverse
	 * @return array
	 */
	private function _getAsArray($var, $inverse = null)
	{
		if (!is_array($var))
		{
			$var = array($var);
			if ($inverse)
			{
				$var = array($var => $inverse);
			}
		}
		return $var;
	}

	/**
	 * Forces a variable to be the key of an
	 * array with the value as $defaultValue
	 *
	 * @param type $var
	 * @param type $defaultValue
	 * @return type
	 */
	private function _getAsInverseArray($var, $defaultValue = 1)
	{
		return $this->_getAsArray($var, $defaultValue);
	}

	/**
	 * DB escape function
	 *
	 * @param string|number $var
	 * @return string|number
	 */
	private function _escape($var)
	{
		if ($var === null)
		{
			return 'NULL';
		}
		return $this->_adapter->getConnection()->real_escape_string(str_replace(array('<', '>'), '',$var));
	}

	/**
	 * Saves the current _parts
	 */
	private function _pushParts()
	{
		$this->_partsBuffer[] = $this->_parts;
	}

	/**
	 * Restores the previous _parts
	 */
	private function _popParts()
	{
		if (is_array($this->_partsBuffer))
		{
			$this->_parts = array_pop($this->_partsBuffer);
		}
	}

	/**
	 * Escapes and formats the function
	 *
	 * @param string|number $str
	 * @return string|number
	 */
	private function _escapeAndFormat($str)
	{
		if ($str === null)
		{
			return 'NULL';
		}
		$result = $this->_escape($str);
		if (!is_numeric($result))
		{
			$result = '"' . $result . '"';
		}
		return $result;
	}

	/**
	 * Private function that prepares the joins and froms
	 *
	 * @param string $type
	 * @param string $name
	 * @param string $cond
	 * @param string $columns
	 * @return \QueryBuilder
	 */
	protected function _joiner($type, $name = array(), $cond = null, $columns = null)
	{
		//Defensive conditions
		if (empty($name))
		{
			$this->_exception('No tables where selected');
		}

		if ($type != self::FROM && empty($cond))
		{
			$this->_exception('No condition specified for ' . self::JOIN);
		}
		//Defensive end
		$name = $this->_getAsArray($name);
		foreach ($name as $_alias => $_table)
		{
			if (strpos($_table, ',') === false)
			{
				$_table = '`' . $_table . '`';
			}
			$subQuery = $_table;
			if (!is_int($_alias))
			{
				$subQuery .= ' ' . self::SQL_AS . ' ' . $_alias;
			}
			if ($cond)
			{
				$subQuery .= ' ' . self::SQL_ON . ' ' . $cond;
			}

			if (self::$_joinSQL[$type] == self::SQL_FROM)
			{
				$this->_parts[self::FROM][] = $subQuery;
			}
			else
			{
				$this->_parts[self::JOIN][] = self::$_joinSQL[$type] . ' ' . $subQuery;
			}

			if ($columns)
			{
				$this->columns($columns, $_alias);
			}
		}

		return $this;
	}

	/**
	 * Private function that prepares the where part.
	 * If value is specified, it escapes it
	 *
	 * @param string $condition
	 * @param string $value
	 * @return string
	 */
	protected function _where($condition, $value = null)
	{
		if (count($this->_parts[self::UNION]))
		{
			$this->_exception('Invalid use of where clause with ' . self::SQL_UNION);
		}

		if ($value !== null) {
			if ((strpos($value, self::SQL_SELECT) !== false) &&
				strpos($value, self::SQL_FROM))
			{
				//We have a query inside a condition, therefore we should
				//not escape it.
				//do Nothing
			}
			else
			{
				$value = $this->_getAsArray($value);
				foreach ($value as &$_value)
				{
					$_value = $this->_escapeAndFormat($_value);
				}
				$value = implode(', ', $value);
			}
			if (strpos($condition, '?') === false)
			{
				$this->_exception('Invalid use of where clause: No `?` found for escaped value');
			}
			$condition = str_replace('?', $value, $condition);
		}

		return $condition;
	}

	/**
	 * Inserts part in the array for
	 *
	 * @param type $key
	 * @param type $arrValues
	 */
	protected function _pushInParts($key,$arrValues)
	{
		$arrValues = $this->_getAsArray($arrValues);
		foreach ($arrValues as $_value)
		{
			$this->_parts[$key][] = $this->_escape($_value);
		}
	}

	/**
	 * Exception treatment
	 *
	 * @param type $message
	 * @param type $type
	 * @throws Exception
	 */
	protected function _exception($message, $type = null)
	{
		throw new Exception($message);
	}

	/* THE MAGIC BEGINS! */

	/**
	 *	SELECT SECTION
	 */

	/**
	 * Prepares the query for a select statement
	 *
	 * @return \QueryBuilder
	 */
	public function select()
	{
		$this->flush();
		$columns = func_get_args();
		$this->_parts['action'] = self::SQL_SELECT;
		$this->columns($columns);
		return $this;
	}

	public function columns($fields = array('*'), $prefix = null)
	{
		$columns = array();
		if ($fields[0] && is_array($fields[0]))
		{
			$fields = $fields[0];
		}
		if (!is_array($fields))
		{
			$fields = $this->_getAsArray($fields);
		}
		foreach ($fields as $_key => $_field)
		{
			$_field = '' . $_field . '';
			if ($prefix)
			{
				$_prefix = '`' . $_prefix . '`';
				$_field = $prefix . '.' . $_field;
			}
			if (!is_int($_key))
			{
				$_field .= ' ' . self::SQL_AS . ' `' . $_key . '`';
			}
			$columns[] = $_field;
		}
		$this->_pushInParts(self::COLUMNS, $columns);
		return $this;
	}

	public function from($tables = array(), $columns = null)
	{
		$this->_joiner(self::FROM, $tables, null, $columns);
		return $this;
	}

	public function join($name, $cond, $columns = null)
	{
		$this->joinInner($name, $cond, $columns);
		return $this;
	}

	public function joinInner($name, $cond, $columns = null)
	{
		$this->_joiner(self::INNER_JOIN, $name, $cond, $columns);
		return $this;
	}

	public function joinLeft($name, $cond, $columns = null)
	{
		$this->_joiner(self::LEFT_JOIN, $name, $cond, $columns);
		return $this;
	}

	public function where($cond, $value = null)
	{
		$this->_parts[self::WHERE][$this->_whereAmbit]['data'][self::SQL_AND][] = $this->_where($cond, $value);
		return $this;
	}

	public function whereOr($cond, $value = null)
	{
		$this->_parts[self::WHERE][$this->_whereAmbit]['data'][self::SQL_OR][] = $this->_where($cond, $value);
		return $this;
	}

	public function whereNewAmbit($typeCondition = self::SQL_AND)
	{
		$this->_whereAmbit++;
		$this->_parts[self::WHERE][$this->_whereAmbit]['cond'] = $typeCondition;
		return $this;
	}

	public function group($spec)
	{
		$this->_pushInParts(self::GROUP, $spec);
		return $this;
	}

	public function having($cond)
	{
		$this->_parts[self::HAVING][] = $this->_escape($cond);
		return $this;
	}

	public function order($cond)
	{
		$this->_parts[self::ORDER][] = $this->_escape($cond);
		return $this;
	}

	public function limit($from, $number = null)
	{
		if ($number === null)
		{
			//From becomes count
			$this->_parts[self::LIMIT_OFFSET] = 0;
			$this->_parts[self::LIMIT_COUNT] = $this->_escape($from);
		}
		else
		{
			$this->_parts[self::LIMIT_OFFSET] = $this->_escape($from);
			$this->_parts[self::LIMIT_COUNT] = $this->_escape($number);
		}
		return $this;
	}

	/**
	 *	INSERT SECTION
	 */
	public function insert()
	{
		$this->flush();
		$values = func_get_args();
		$this->_parts['action'] = self::SQL_INSERT;
		if ($values[0])
		{
			$this->into($values[0]);
		}
		if ($values[1])
		{
			$this->values($values[1]);
		}
		return $this;
	}

	public function into($name = null)
	{
		if ($name === null)
		{
			$this->_exception('No ' . self::SQL_INTO . ' statement specified.');
		}
		if (!is_string($name))
		{
			$this->_exception('Wrong ' . self::SQL_INTO . ' type passed to method. String expected.');
		}
		$this->_parts[self::INTO] = $name;
		return $this;
	}

	public function values($fields = array())
	{
		$columns = array();
		$values = array();
		if ($fields[0] && is_array($fields[0]))
		{
			$fields = $fields[0];
		}
		if (!is_array($fields))
		{
			$fields = $this->_getAsArray($fields);
		}
		foreach ($fields as $_key => $_field)
		{
			if (!is_int($_key))
			{
				switch ($this->_parts['action'])
				{
					case self::SQL_INSERT:
						$this->_parts[self::VALUES][$this->_escape($_key)] = $this->_escapeAndFormat($_field);
						break;

					case self::SQL_UPDATE:
						$this->_parts[self::VALUES][] = '`' . $this->_escape($_key) . '` = ' . $this->_escapeAndFormat($_field);
						break;
				}
			}
			else
			{
				$this->_exception('Wrong "key" for ' . self::SQL_VALUES . ' construction. No column name specified.');
			}
		}
		return $this;
	}

	/**
	 *  UPDATE SECTION
	 */
	public function update()
	{
		$this->flush();
		$values = func_get_args();
		$this->_parts['action'] = self::SQL_UPDATE;
		if ($values[0])
		{
			$this->table($values[0]);
		}
		return $this;
	}

	public function table($arg)
	{
		return $this->into($arg);
	}

	public function set($args = array())
	{
		return $this->values($args);
	}

	/*
	 * DELETE SECTION
	 */
	public function delete()
	{
		$this->flush();
		$values = func_get_args();
		$this->_parts['action'] = self::SQL_DELETE;
		if ($values[0])
		{
			$this->from($values[0]);
		}
		return $this;
	}

	/*
	 * GET QUERY
	 */
	public function getQuery($returnAndClean = false, $partSeparator = self::CARRY_RETURN)
	{
		$query = '';
		switch ($this->_parts['action'])
		{
			case self::SQL_SELECT:
				$query .= self::SQL_SELECT;
				//COLUMNS
				if (empty($this->_parts[self::COLUMNS]))
				{
					$this->_parts[self::COLUMNS][] = self::SQL_WILDCARD;
				}
				$query .= ' ' . implode(', ', $this->_parts[self::COLUMNS]) . ' ' . $partSeparator;
				//FROM and JOINs
				$query .= self::SQL_FROM . ' ' . implode(', ', $this->_parts[self::FROM]) . ' ' . $partSeparator;
				if ($this->_parts[self::JOIN])
				{
					$query .= implode( ' ' . $partSeparator, $this->_parts[self::JOIN]) . ' ' . $partSeparator;
				}
				//WHERE
				if ($this->_parts[self::WHERE])
				{
					$query .= self::SQL_WHERE . ' ';
					foreach ($this->_parts[self::WHERE] as $_ambit)
					{
						$query .= ($_ambit['cond']) ? $_ambit['cond'] . $partSeparator : '';
						$query .= '(';
						$partialWhere = array();
						if ($_ambit['data'][self::SQL_AND])
						{
							$partialWhere[] .= implode(' ' . self::SQL_AND . ' ', $_ambit['data'][self::SQL_AND]);
						}
						if ($_ambit['data'][self::SQL_OR])
						{
							$partialWhere[] .= implode(' ' . self::SQL_OR . ' ', $_ambit['data'][self::SQL_OR]);
						}
						$query .= implode(' ' . self::SQL_OR . ' ', $partialWhere);
						$query .= ') ';
					}
					$query .= $partSeparator;
				}
				//GROUP BY
				if ($this->_parts[self::GROUP])
				{
					$query .= self::SQL_GROUP_BY . ' ' . implode(', ', $this->_parts[self::GROUP]) . $partSeparator;
				}
				//ORDER BY
				if ($this->_parts[self::ORDER])
				{
					$query .= self::SQL_ORDER_BY . ' ' . implode(', ', $this->_parts[self::ORDER]) . $partSeparator;
				}
				//LIMIT
				if ($this->_parts[self::LIMIT_COUNT])
				{
					$query .= self::SQL_LIMIT . ' ' . $this->_parts[self::LIMIT_OFFSET] . ', ' . $this->_parts[self::LIMIT_COUNT];
				}
				if ($partSeparator == self::CARRY_RETURN)
				{
					$query .= self::SQL_QUERY_DIVISOR;
				}
				break;

			case self::SQL_INSERT:
				//INSERT INTO
				$query = self::SQL_INSERT . ' ' . self::SQL_INTO . $partSeparator;
				$query .= '`' . $this->_parts[self::INTO] . '` (`' . implode('`, `', array_keys($this->_parts[self::VALUES])) . '`) ' . $partSeparator;
				$query .= self::SQL_VALUES . ' (' . implode(', ', $this->_parts[self::VALUES]) . ')';
				if ($partSeparator == self::CARRY_RETURN)
				{
					$query .= self::SQL_QUERY_DIVISOR;
				}
				break;

			case self::SQL_UPDATE:
				//UPDATE
				$query = self::SQL_UPDATE . ' `' . $this->_parts[self::INTO] . '` ' . $partSeparator;
				$query .= self::SQL_SET . ' ' . implode(', ', $this->_parts[self::VALUES]) . ' ' . $partSeparator;
				if ($this->_parts[self::WHERE])
				{
					$query .= self::SQL_WHERE . ' ';
					foreach ($this->_parts[self::WHERE] as $_ambit)
					{
						$query .= ($_ambit['cond']) ? $_ambit['cond'] . $partSeparator : '';
						$query .= '(';
						$partialWhere = array();
						if ($_ambit['data'][self::SQL_AND])
						{
							$partialWhere[] .= implode(' ' . self::SQL_AND . ' ', $_ambit['data'][self::SQL_AND]);
						}
						if ($_ambit['data'][self::SQL_OR])
						{
							$partialWhere[] .= implode(' ' . self::SQL_OR . ' ', $_ambit['data'][self::SQL_OR]);
						}
						$query .= implode(' ' . self::SQL_OR . ' ', $partialWhere);
						$query .= ') ';
					}
				}
				if ($partSeparator == self::CARRY_RETURN)
				{
					$query .= self::SQL_QUERY_DIVISOR;
				}
				break;

			case self::SQL_DELETE:
				//DELETE
				$query = self::SQL_DELETE . ' ' . self::SQL_FROM . ' ' . $this->_parts[self::FROM][0] . ' ' . $partSeparator;
				if ($this->_parts[self::WHERE])
				{
					$query .= self::SQL_WHERE . ' ';
					foreach ($this->_parts[self::WHERE] as $_ambit)
					{
						$query .= ($_ambit['cond']) ? $_ambit['cond'] . $partSeparator : '';
						$query .= '(';
						$partialWhere = array();
						if ($_ambit['data'][self::SQL_AND])
						{
							$partialWhere[] .= implode(' ' . self::SQL_AND . ' ', $_ambit['data'][self::SQL_AND]);
						}
						if ($_ambit['data'][self::SQL_OR])
						{
							$partialWhere[] .= implode(' ' . self::SQL_OR . ' ', $_ambit['data'][self::SQL_OR]);
						}
						$query .= implode(' ' . self::SQL_OR . ' ', $partialWhere);
						$query .= ') ';
					}
				}
				if ($partSeparator == self::CARRY_RETURN)
				{
					$query .= self::SQL_QUERY_DIVISOR;
				}
				break;
		}

		if ($returnAndClean)
		{
			$this->_lastQuery = $query;
			$this->_popParts();
		}

		return $query;
	}

	/**
	 * Cleans the _parts array preparing it
	 * for a new queryBuilder call
	 */
	public function flush()
	{
		if (!empty($this->_parts))
		{
			$this->_pushParts();
		}
		unset($this->_parts);
	}

	public function __toString()
	{
		return $this->getQuery();
	}

	/**
	 *
	 *
	 * @param string $cacheId
	 * @param int $cacheTime
	 * @return object
	 */

	public function queryExec($cacheId = NULL, $cacheTime = NULL)
	{
		$formatting = null;
		if ($this->_parts['formatting'])
		{
			$formatting = $this->_parts['formatting'];
		}

		$result = $this->_adapter->query($this->getQuery(true), $cacheId, $cacheTime);

		switch ($formatting)
		{
			case 'array':
				foreach ($result as &$_value)
				{
					$_value = (array) $_value;
				}
				break;
		}

		return $result;
	}

	/**
	 * Returns the last query executed
	 *
	 * @return string
	 */
	public function getLastQuery()
	{
		return $this->_lastQuery;
	}



	public function subquery()
	{
		return $this->getQuery(false, '');
	}

	/**
	 * Specifies a format for the receiving data from DB
	 *
	 * @param string $type
	 * @return \QueryBuilder
	 */
	public function formatAs($type = null)
	{
		switch ($type)
		{

			case 'array':
			case 'Array':
			case 'arr':
				$this->_parts['formatting'] = 'array';
				break;

			default:
				$this->_exception('Invalid formatting type in `formatAs`');
				break;
		}

		return $this;
	}

	public function toArray()
	{
		return $this->formatAs('array');
	}

	/*
	 * TRANSACTIONS
	 */

	public function begin()
	{
		return $this->_adapter->query(self::SQL_BEGIN . self::SQL_QUERY_DIVISOR);
	}

	public function commit()
	{
		return $this->_adapter->query(self::SQL_COMMIT . self::SQL_QUERY_DIVISOR);
	}

	public function rollback()
	{
		return $this->_adapter->query(self::SQL_ROLLBACK . self::SQL_QUERY_DIVISOR);
	}

}
