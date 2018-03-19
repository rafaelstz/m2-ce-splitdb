
[![MageMojo](https://magetalk.com/wp-content/uploads/2017/11/q7xJZaM5TImMN7mUIb0c.png)](https://magemojo.com/)

# SplitDb
#### Magento 2 module to redirect SELECT queries to another database connection.  

![Version 1.5.0](https://img.shields.io/badge/Version-1.5.0-green.svg)

## Contributing
See [CONTRIBUTING.md](CONTRIBUTING.md).

## Manual Install

- [Download this ZIP](https://github.com/magemojo/m2-ce-splitdb/archive/master.zip) and paste in your root folder.

- Add a new connection in your file `app/etc/env.php` called **readonly** and **readonly_setup**, like the image below.

![MageMojo SplitDb](https://user-images.githubusercontent.com/610598/37181799-4268c930-230d-11e8-89f8-355142b60db5.png)

- Run these commands in your terminal:

```bash
bin/magento module:enable MageMojo_SplitDb
bin/magento setup:upgrade
```

## License
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)


