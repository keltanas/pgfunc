# pgfunc
PHP library for interacting with PostgreSQL connections, transactions and stored procedures

## Integration with Symfony 3.4 and php 7.1

Add to composer.json custom repository (if you use fork)

```json
{
    "repositories": {
        "red-defender/pgfunc": {
            "type": "git",
            "url": "git@github.com:keltanas/pgfunc.git"
        }
    }
}
```

Include dependency

```bash
composer require red-defender/pgfunc
```

Open `app/config/services.yml` file and paste next code:

```yaml
services:
    # ...
    
    #
    # PgFunc Services
    #
    vendorname_stored_procedure.pg_func.configuration:
        class: PgFunc\Configuration
        calls:
            - ["setIsTransactionEnabled", [true]]
        public: true

    vendorname_stored_procedure.pg_func.connection:
        class: PgFunc\Connection
        arguments:
            - "@vendorname_stored_procedure.pg_func.configuration"
            - "@doctrine.dbal.default_connection"
        public: true
```

## Using with Symfony 3.4

Write to `app/config/services.yml`

```yaml
services:
    # ...

    #
    # Managers
    #
    app.doctrine.stored_procedure:
        class: AppBundle\Doctrine\StoredProcedureManager
        arguments:
            - "@vendorname_stored_procedure.pg_func.connection"
        public: true

```

And use

```php
<?php
namespace AppBundle\Doctrine;

use PgFunc\Connection;
use PgFunc\Exception;
use PgFunc\Exception\Usage;
use PgFunc\Mapper;
use PgFunc\Procedure;

/**
 * Class TransactionTokenExistsException
 * @package AppBundle\Exception\Managers\Balance
 */
class TransactionTokenExistsException extends \Exception
{
    const MESSAGE = 'Transaction token already created.';
}

class StoredProcedureManager
{
    /** @var Connection */
    private $pgFunc;

    /**
     * StoredProcedureManager constructor.
     *
     * @param Connection $pgFunc
     *
     * @throws \Doctrine\DBAL\DBALException
     */
    public function __construct(Connection $pgFunc)
    {
        $this->pgFunc = $pgFunc;
    }

    /**
     * @return Connection
     */
    private function getPgFunc()
    {
        return $this->pgFunc;
    }

    /**
     * @param string $transactionToken
     * @param string $objectType
     * @param int $objectId
     *
     * @throws Exception\Usage
     */
    public function myStoredProcedureCaller(
        string $transactionToken,
        string $objectType,
        int $objectId
    ) : void {
        $procedure = new Procedure('my_stored_procedure');

        $procedure->addParameter('p_transaction_token', Mapper::VARCHAR);
        $procedure->addParameter('p_object_type', Mapper::VARCHAR);
        $procedure->addParameter('p_object_id', Mapper::BIGINT);

        $procedure->setReturnType(Procedure::RETURN_VOID);

        $procedure->setErrorMap([
            TransactionTokenExistsException::MESSAGE => TransactionTokenExistsException::class,
        ]);

        $procedure->setData('p_transaction_token', $transactionToken);
        $procedure->setData('p_object_type', $objectType);
        $procedure->setData('p_object_id', $objectId);

        try {
            $this->getPgFunc()
                ->queryProcedure($procedure);
        } catch (Exception\Database $exception) {
            throw new \Exception($exception->getMessage(), $exception->getCode(), $exception);
        }
    }
}
```
