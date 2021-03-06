# PDOBatch
Three classes for batch-processing of CUD-Operations with PDOs prepared statements, which provide a massive performance boost compared to single statement executions.

## Usage
```php
require_once("class.pdobatch.php");

$db = new PDO
(
  "mysql:host=127.0.0.1;dbname=test;", "root", "password",
  array(PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION)
);

// ================================================================
// Create a batch inserter for the table users for many records of
// which you want to set the columns "surname", "lastname" 
// and "email", where every batch will consist of 600 records.
$bi = new PDOBatch\PDOBatchInserter($db, "users", ["surname","lastname","email"], 600);

for($i=0; $i<10000; $i++)
{
  // This only does a real db-insert on every collected 600 records,
  // empties the collection subsequently, before restarting to
  // collect again.
  $bi->addBatch(["foo$i", "bar$i", "baz$i"]);
}

// This has to be done always at the end to insert remaining
// records. In our case its 10000 % 600 which results in 400 records
// remaining to insert.
$bi->finalize();


// ================================================================
// Create a batch updater for the table users.
// In every row the column "subscribed" will be set to 1 if the
// surname- and email-columns match the criterias passed to the
// addBatch() method further down.
$bu = new PDOBatch\PDOBatchUpdater(
  $db,
  "users",
  ["subscribed"],
  [1],
  ["surname", "email"],
  700
);

// Only set the column "subscribed" to 1 if surname matches foo0-foo5000
// AND email matches baz1-baz5000 .
for($i=0; $i<5000; $i++)
{
  $bu->addBatch(["foo$i","baz$i"], "AND");
}

// Update the remaining 100 records (5000 % 700)
$bu->finalize();


// ================================================================
// Create a batch deleter for the table users.
// Delete all users whose surname index is dividable by 3.
$bd = new PDOBatch\PDOBatchDeleter($db, "users", ["surname"], 500);
for($i=0; $i<10000; $i++)
{
  if ($i % 3 == 0)
    $bd->addBatch(["foo$i"]);
}
$bd->finalize();
```

## Benchmarks

```sql
CREATE TABLE test.users (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `surname` varchar(45) DEFAULT NULL,
  `lastname` varchar(45) DEFAULT NULL,
  `email` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id_UNIQUE` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=133214 DEFAULT CHARSET=latin1;
```

```php
// https://github.com/veloper/Bench/blob/master/class.Bench.php
require_once("class.Bench.php");

$countIandU = 200;      // insert and update count
$countU = 50;           // update count

$batchsizeIandD = 50;   // insert and update max batch size
$batchsizeU = 20;       // update max batch size

$db = new PDO("mysql:host=127.0.0.1;dbname=test;", "root", "password",array(PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION));

$db->exec("DELETE FROM users");

// ======================================================
// Benchmark with batch classes (~0.2 sec)
// ======================================================
Bench::start();

$bi = new PDOBatch\PDOBatchInserter($db, "users", ["surname","lastname","email"], $batchsizeIandD);
for($i=0; $i<$countIandU; $i++)
	$bi->addBatch(["foo".$i, "bar".$i, "baz".$i]);
$bi->finalize();

$bd = new PDOBatch\PDOBatchDeleter($db, "users", ["surname"], $batchsizeIandD);
for($i=0; $i<$countIandU; $i++)
{
	// delete every 3rd row
	if ($i % 3 == 0)
		$bd->addBatch(["foo$i"]);
}
$bd->finalize();

$bu = new PDOBatch\PDOBatchUpdater(
	$db,
	"users",
	["lastname"],
	["Threepwood"],
	["surname", "email"],
	$batchsizeU
);

for($i=0; $i<$countU; $i++)
{
	// update the first $ucount rows 
	$bu->addBatch(["foo$i","baz$i"]);
}

$bu->finalize();

Bench::stop();

// ======================================================
// Benchmark with single statements (~6 sec)
// ======================================================
Bench::reset();

$db->exec("DELETE FROM users");

Bench::start();

$stmt = $db->prepare("INSERT INTO users(surname,lastname,email) VALUES(?,?,?)");
for($i=0; $i<$countIandU; $i++)
	$stmt->execute(["foo$i", "bar$i", "baz$i"]);
$stmt = NULL;

$stmt = $db->prepare("DELETE FROM users WHERE surname=?");
for($i=0; $i<$countIandU; $i++)
{
	if ($i % 3 == 0)
		$stmt->execute(["foo$i"]);
}
$stmt = NULL;

$stmt = $db->prepare("UPDATE users SET surname='Threepwood' WHERE surname=? AND lastname=?");

for($i=0; $i<$countU; $i++)
{
	$stmt->execute(["foo$i", "baz$i"]);
}
$stmt = NULL;
Bench::stop();

print_r(Bench::getElapsed());
```
