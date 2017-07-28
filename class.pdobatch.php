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
			public function addBatch($values);
			public function finalize();
		}

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
			private $conditionstatement;

			function __construct($db, $table, $updatecolumns, $updatevalues, $conditioncolumns, $conditionstatement, $maxbatch, $stmtDriverOptions = NULL)
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
				if (!PDOBatchHelpers::isStr($conditionstatement ) || ($conditionstatement !== "OR" && $conditionstatement != "AND"))
					throw new \Exception("Parameter conditionstatement is invalid");

				$this->db = $db;
				$this->maxbatch = $maxbatch;
				$this->stmtDriverOptions = $stmtDriverOptions;
				$this->queryValues = $updatevalues;
				$this->queryIntro = "UPDATE " . $table . " SET ";
				$this->conditioncolumncount = count($conditioncolumns);
				$this->conditioncolumns = $conditioncolumns;
				$this->conditionstatement = $conditionstatement;

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

			public function addBatch($values)
			{
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
					if ($i > 0) $conditions .= " $this->conditionstatement ";
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

			public function finalize()
			{
				if ($this->completeBatchCount % $this->maxbatch != 0)
					$this->execute();
				$this->reset();
			}
		}

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
					if ($i > 0) $conditions .= ",";
					$conditions .= "$columnname=?";
					$i++;
				}
				$this->queryConditions = $conditions;
			}

			public function addBatch($values)
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

			private function reset()
			{
				$this->currentBatchCount = 0;
				$this->batchQueryConditions = "";
				unset($this->queryArray);
				$this->queryArray = [];
			}

			public function finalize()
			{
				if ($this->completeBatchCount % $this->maxbatch != 0)
					$this->execute();
				$this->reset();
			}
		}

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

			public function addBatch($values)
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

			private function reset()
			{
				$this->currentBatchCount = 0;
				$this->queryOutro = "";
				unset($this->queryArray);
				$this->queryArray = [];
			}

			public function finalize()
			{
				if ($this->completeBatchCount % $this->maxbatch != 0)
					$this->execute();
				$this->reset();
			}
		}
	}
