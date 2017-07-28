<?php

	namespace PDOBatch
	{
		class PDOBatchHelpers
		{
			public static function isEmptyVar($var) { return (!isset($var) || is_null($var)); }
			public static function isArray($var) { return (!PDOBatchHelpers::isEmptyVar($var) && is_array($var)); }
			public static function isEmptyArray($var){ return (!PDOBatchHelpers::isArray($var) || count($var) === 0); }
			public static function isInt($var) { return (!PDOBatchHelpers::isEmptyVar($var) && is_int($var)); }
			public static function isStr($var) { return (!PDOBatchHelpers::isEmptyVar($var) && is_string($var));}
		}

		interface IPDOBatch
		{
			public function addBatch($values, $conditionoperator=NULL);
			public function finalize();
		}

		/**
		 * A class for executing prepared UPDATE statements on multiple rows
		 * with different criterias in a faster way than with single executes
		 * @package PDOBatch
		 */
		class PDOBatchUpdater implements IPDOBatch
		{
			private $db;
			private $maxbatch;
			private $stmtDriverOptions;
			private $queryIntro;
			private $queryArray = [];
			private $queryValues;
			private $batchQueryConditions = "";
			private $currentBatchCount = 0;
			private $completeBatchCount = 0;
			private $conditioncolumncount;
			private $conditioncolumns;
			private $conditionoperator;

			/**
			 * Constructor
			 *
			 * @param PDO $db                       An initialized PDO-Instance
			 * @param string $table                 The name of the table you want to UPDATE
			 * @param string[] $updatecolumns       The name of columns to update.
			 *                                      Has to be the same array-length as $updatevalues.
			 *                                          SET $updatecolumns[0]=?,$updatecolumns[1]=?...
			 * @param object[] $updatevalues        The values of the columns to update.
			 *                                      Has to be the same array-length as $updatecolumns.
			 *                                          SET columnname1=$updatevalues[0], columnname2=$updatevalues[1]...
			 * @param string[] $conditioncolumns    The name of condition-columns
			 *                                          WHERE ($conditioncolumns[0]=? AND/OR $conditioncolumns[1]=?...) OR
			 *                                                ($conditioncolumns[0]=? AND/OR $conditioncolumns[1]=?...)
			 *                                      Has to be the same array-length as the parameter for $this->addBatch()
			 * @param $maxbatch                     The maximum number of rows to UPDATE per batch
			 * @param  $stmtDriverOptions           Driver options to pass to the prepared statement
			 *                                      see http://php.net/manual/de/pdo.prepare.php
			 * @throws \Exception                   If some parameters are invalid
			 */
			function __construct($db, $table, $updatecolumns, $updatevalues, $conditioncolumns, $maxbatch, $stmtDriverOptions = NULL)
			{
				if ($db === NULL)
					throw new \Exception("Parameter db is invalid");
				if (PDOBatchHelpers::isEmptyArray($updatecolumns))
					throw new \Exception("Parameter updatecolumns is not an array");
				if (PDOBatchHelpers::isEmptyArray($updatevalues))
					throw new \Exception("Parameter updatevalues is not an array");
				if (count($updatecolumns) != count($updatevalues))
					throw new \Exception("Parameters updatecolumns and updatevalues are not of same size");
				if (PDOBatchHelpers::isEmptyArray($conditioncolumns))
					throw new \Exception("Parameter conditioncolumns is not an array");
				if (!PDOBatchHelpers::isInt($maxbatch) || $maxbatch < 1)
					throw new \Exception("Parameter maxbatch has to be an int > 0");

				$this->db = $db;
				$this->maxbatch = $maxbatch;
				$this->stmtDriverOptions = $stmtDriverOptions;
				$this->queryValues = $updatevalues;
				$this->queryIntro = "UPDATE " . $table . " SET ";
				$this->conditioncolumncount = count($conditioncolumns);
				$this->conditioncolumns = $conditioncolumns;

				$updatecolumnstr = "";
				$i = 0;
				foreach ($updatecolumns as $updatecolumn)
				{
					if ($i > 0) $updatecolumnstr .= ",";
					$updatecolumnstr .= "$updatecolumn=?";
					$i++;
				}
				$this->queryIntro .= $updatecolumnstr." WHERE ";
				foreach ($updatevalues as $updatevalue)
				{
					array_push($this->queryArray, $updatevalue);
				}
			}

			/**
			 * Adds an UPDATE batch-item for a single row-condition.
			 * If $this->maxbatch (see constructor) is reached it executes the collected batch-items.
			 *
			 * @param object[] $values              The values of the row-condition
			 *                                          WHERE (conditioncolum0=$value[0] AND/OR conditioncolumn1=$value[1]...) OR
			 *                                                (conditioncolum0=$value[0] AND/OR conditioncolumn1=$value[1]...)
			 * @param string $conditionoperator     The operator with which to concat single row conditions (AND or OR, default=AND)
			 * @return boolean                      TRUE if an execution of collected batch items was successful
			 *                                      or if the passed batch-item could be added otherwise FALSE
			 * @throws \Exception                   If a method-parameter was invalid or PDO throws out
			 */
			public function addBatch($values, $conditionoperator="AND")
			{
				if (!PDOBatchHelpers::isStr($conditionoperator ) || (strtolower($conditionoperator) !== "or" && strtolower($conditionoperator) != "and"))
					throw new \Exception("Parameter conditionstatement is invalid");

				$this->conditionoperator = $conditionoperator;

				if (PDOBatchHelpers::isEmptyArray($values))
					throw new \Exception("values-array is empty");

				if ($this->conditioncolumncount != count($values))
					throw new \Exception("conditioncolumncount is not the same size as values-array");

				$conditions = "";
				$i = 0;
				if ($this->currentBatchCount > 0)
					$conditions .= " OR ";
				foreach ($values as $value)
				{
					array_push($this->queryArray, $value);
					if ($i > 0) $conditions .= " $this->conditionoperator ";
					$conditions .= $this->conditioncolumns[$i]."=?";
					$i++;
				}
				$this->batchQueryConditions .= $conditions;
				$this->currentBatchCount++;
				$this->completeBatchCount++;
				if ($this->currentBatchCount === $this->maxbatch)
					return $this->execute();
				return TRUE;
			}

			/**
			 * Called everytime a $this->maxbatch is reached within $this->addbatch()
			 * @return mixed
			 * @throws \Exception If PDO throws out
			 */
			private function execute()
			{
				if (!PDOBatchHelpers::isEmptyArray($this->stmtDriverOptions))
					$stmt = $this->db->prepare($this->queryIntro . $this->batchQueryConditions, $this->stmtDriverOptions);
				else
					$stmt = $this->db->prepare($this->queryIntro . $this->batchQueryConditions);
				$success = $stmt->execute($this->queryArray);
				$this->reset();
				return $success;
			}

			/**
			 * Called after every PDO-Statement execution
			 */
			private function reset()
			{
				$this->currentBatchCount = 0;
				$this->batchQueryConditions = "";
				unset($this->queryArray);
				$this->queryArray = [];
				foreach ($this->queryValues as $updatevalue)
				{
					array_push($this->queryArray, $updatevalue);
				}
			}

			/**
			 * This has to be called finally after all $this->addBatch() calls have been made,
			 * to execute the statements for the leftover batch-items
			 */
			public function finalize()
			{
				if ($this->completeBatchCount % $this->maxbatch != 0)
					$this->execute();
				$this->reset();
			}
		}

		/**
		 * A class for executing prepared DELETE statements for rows with different criterias
		 * @package PDOBatch
		 */
		class PDOBatchDeleter implements IPDOBatch
		{
			private $db;
			private $maxbatch;
			private $stmtDriverOptions;
			private $queryIntro;
			private $queryConditions;
			private $queryArray = [];
			private $batchQueryConditions = "";
			private $currentBatchCount = 0;
			private $completeBatchCount = 0;

			/**
			 * Constructor
			 *
			 * @param PDO $db                       An initialized PDO-Instance
			 * @param string $table                 The name of the table you want to DELETE from
			 * @param string[] $columnnames         The name of the columns that will be used for every delete-criterium
			 *                                          DELETE FROM x
			 *                                          WHERE ($columnnames[0]=? AND/OR $columnnames[1]=?) OR
			 *                                                ($columnnames[0]=? AND/OR $columnnames[1]=?)
			 * @param integer $maxbatch             The maximum number of rows to DELETE per batch
			 * @param string $conditionoperato      The conditionoperator inside the conditions
			 *                                          DELETE FROM x
			 *                                                WHERE (columnname1=? $conditionoperator columnname2=?) OR
			 *                                                (columnname1=? $conditionoperator columnname2=?)
			 * @param object[] $stmtDriverOptions   Driver options to pass to the prepared statement
			 *                                      see http://php.net/manual/de/pdo.prepare.php
			 * @throws \Exception                   If some parameters are invalid
			 */
			function __construct($db, $table, $columnnames, $maxbatch, $stmtDriverOptions = NULL)
			{
				if ($db === NULL)
					throw new \Exception("Parameter db is invalid");
				if (PDOBatchHelpers::isEmptyArray($columnnames))
					throw new \Exception("Parameter columnnames is not an array");
				if (!PDOBatchHelpers::isInt($maxbatch) || $maxbatch < 1)
					throw new \Exception("Parameter maxbatch has to be an int > 0");

				$this->db = $db;
				$this->queryIntro = "DELETE FROM " . $table . " WHERE ";
				$this->maxbatch = $maxbatch;
				$this->stmtDriverOptions = $stmtDriverOptions;

				$conditions = "";
				$i = 0;
				foreach ($columnnames as $columnname)
				{
					if ($i > 0) $conditions .= " $conditionoperator ";
					$conditions .= "$columnname=?";
					$i++;
				}
				$this->queryConditions = $conditions;
			}
			
			/**
			 * Adds an DELETE batch-item for a single row-condition.
			 * If $this->maxbatch (see constructor) is reached it executes the collected batch-items.
			 * @param object[] $values              The values of the row-condition
			 *                                          WHERE (conditioncolum0=$value[0] AND/OR conditioncolumn1=$value[1]...) OR
			 *                                                (conditioncolum0=$value[0] AND/OR conditioncolumn1=$value[1]...)
			 * @param string $conditionoperator     Not used here
			 * @return boolean                      TRUE if an execution of collected batch items was successful or
			 *                                      if the passed batch-item could be added otherwise FALSE
			 * @throws \Exception                   If PDO throws out
			 */
			public function addBatch($values, $conditionoperator=NULL)
			{
				foreach ($values as $value)
				{
					array_push($this->queryArray, $value);
				}
				$this->batchQueryConditions .= (($this->currentBatchCount > 0) ? " OR " : "") . $this->queryConditions;
				$this->currentBatchCount++;
				$this->completeBatchCount++;
				if ($this->currentBatchCount === $this->maxbatch)
					return $this->execute();
				return TRUE;
			}

			/**
			 * Called everytime a $this->maxbatch is reached within $this->addbatch()
			 * @return mixed
			 * @throws \Exception If PDO throws out
			 */
			private function execute()
			{
				if (!PDOBatchHelpers::isEmptyArray($this->stmtDriverOptions))
					$stmt = $this->db->prepare($this->queryIntro . $this->batchQueryConditions, $this->stmtDriverOptions);
				else
					$stmt = $this->db->prepare($this->queryIntro . $this->batchQueryConditions);
				$success = $stmt->execute($this->queryArray);
				$this->reset();
				return $success;
			}

			/**
			 * Called after every PDO-Statement execution
			 */
			private function reset()
			{
				$this->currentBatchCount = 0;
				$this->batchQueryConditions = "";
				unset($this->queryArray);
				$this->queryArray = [];
			}

			/**
			 * This has to be called finally after all $this->addBatch() calls have been made,
			 * to execute the statements for the leftover batch-items
			 */
			public function finalize()
			{
				if ($this->completeBatchCount % $this->maxbatch != 0)
					$this->execute();
				$this->reset();
			}
		}

		/**
		 * A class for executing prepared INSERT statements for multiple rows
		 * @package PDOBatch
		 */
		class PDOBatchInserter implements IPDOBatch
		{
			private $db;
			private $maxbatch;
			private $stmtDriverOptions;
			private $queryIntro;
			private $querySinglePlaceholders;
			private $queryOutro = "";
			private $queryArray = [];
			private $currentBatchCount = 0;
			private $completeBatchCount = 0;

			/**
			 * Constructor
			 *
			 * @param PDO $db                       An initialized PDO-Instance
			 * @param string $table                 The name of the table you want to INSERT into
			 * @param string[] $columnnames         The names of the columns you want to INSERT values for
			 * @param integer $maxbatch             The maximum number of rows to update per batch
			 * @param object[] $stmtDriverOptions   Driver options to pass to the prepared statement
			 *                                      see http://php.net/manual/de/pdo.prepare.php
			 * @throws \Exception
			 */
			function __construct($db, $table, $columnnames, $maxbatch, $stmtDriverOptions = NULL)
			{
				if ($db === NULL)
					throw new \Exception("Parameter db is invalid");
				if (PDOBatchHelpers::isEmptyArray($columnnames))
					throw new \Exception("Parameter columnnames is not an array");
				if (!PDOBatchHelpers::isInt($maxbatch) || $maxbatch < 1)
					throw new \Exception("Parameter maxbatch has to be an int > 0");

				$this->db = $db;
				$prefix = "INSERT INTO " . $table . "(";
				$this->maxbatch = $maxbatch;
				$this->stmtDriverOptions = $stmtDriverOptions;
				$params = "";
				for ($i=0; $i<count($columnnames); $i++)
				{
					$columnname = $columnnames[$i];
					if ($i > 0)
					{
						$prefix .= ",";
						$params .= ",";
					}
					$prefix .= $columnname;
					$params .= "?";
				}
				$prefix .= ") VALUES";
				$this->queryIntro = $prefix;
				$this->querySinglePlaceholders = "(" . $params . ")";
				$this->reset();
			}

			/**
			 * Adds an INSERT batch-item for a single row.
			 * If $this->maxbatch (see constructor) is reached it executes the collected batch-items.
			 * @param object[] $values      The values you want to INSERT
			 *                                  INSERT INTO x(a,b,c) VALUES($values[0],$values[1],$values[2])
			 * @param null $conditions      This parameter is not used, therefore can be ommitted
			 * @return bool                 TRUE if an execution of collected batch items was successful or
			 *                              if the passed batch-item could be added otherwise FALSE
			 */
			public function addBatch($values, $conditions=NULL)
			{
				foreach ($values as $value)
				{
					array_push($this->queryArray, $value);
				}
				$this->queryOutro .= (($this->currentBatchCount > 0) ? "," : "") . $this->querySinglePlaceholders;
				$this->currentBatchCount++;
				$this->completeBatchCount++;
				if ($this->currentBatchCount === $this->maxbatch)
					return $this->execute();
				return TRUE;
			}

			/**
			 * Called everytime a $this->maxbatch is reached within $this->addbatch()
			 * @return boolean
			 * @throws \Exception If PDO throws out
			 */
			private function execute()
			{
				if (!PDOBatchHelpers::isEmptyArray($this->stmtDriverOptions))
					$stmt = $this->db->prepare($this->queryIntro . $this->queryOutro, $this->stmtDriverOptions);
				else
					$stmt = $this->db->prepare($this->queryIntro . $this->queryOutro);

				$success = $stmt->execute($this->queryArray);
				$this->reset();
				return $success;
			}

			/**
			 * Called after every PDO-Statement execution
			 */
			private function reset()
			{
				$this->currentBatchCount = 0;
				$this->queryOutro = "";
				unset($this->queryArray);
				$this->queryArray = [];
			}

			/**
			 * This has to be called finally after all $this->addBatch() calls have been made,
			 * to execute the statements for the leftover batch-items
			 */
			public function finalize()
			{
				if ($this->completeBatchCount % $this->maxbatch != 0)
					$this->execute();
				$this->reset();
			}
		}
	}
?>
