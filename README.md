
[![MageMojo](https://magetalk.com/wp-content/uploads/2017/11/q7xJZaM5TImMN7mUIb0c.png)](https://magemojo.com/)

# SplitDb
#### Magento 2 module to redirect SELECT queries to another database connection.  

![Version 1.5.0](https://img.shields.io/badge/Version-1.5.0-green.svg)

## Contributing
See [CONTRIBUTING.md](CONTRIBUTING.md).

## Manual Install

- [Download this ZIP](https://github.com/magemojo/m2-ce-splitdb/archive/master.zip) and paste in your root folder.

- Add a new connection in your file `app/etc/env.php` called **readonly** and **readonly_setup**, like the example env.php file below.

```
'db' =>
  array (
    'table_prefix' => '',
    'connection' =>
    array (
      'default' =>
      array (
        'host' => 'default_writer',
        'dbname' => 'db_name',
        'username' => 'user_name',
        'password' => 'yourpassword',
        'active' => '1',
      ),

      'readonly' =>
      array (
        'host' => 'reader_host',
        'dbname' => 'db_name',
        'username' => 'user_name',
        'password' => 'yourpassword',
        'active' => '1',
      ),

    ),
  ),
```

- Run these commands in your terminal:

```bash
bin/magento module:enable MageMojo_SplitDb
bin/magento setup:upgrade
```

## License
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)


