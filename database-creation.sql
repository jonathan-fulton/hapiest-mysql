/* Execute by hand to create the required local database for testing purposes */

DROP DATABASE IF EXISTS `hapiestmysql`;
CREATE DATABASE IF NOT EXISTS `hapiestmysql` DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;

/* User for API server connecting to database when collocated; probably only used for local dev */
GRANT ALL ON `hapiestmysql`.* to 'hapiestmysql'@'%' IDENTIFIED BY 'hapiestmysql';
GRANT ALL ON `hapiestmysql`.* to 'hapiestmysql'@'localhost' IDENTIFIED BY 'hapiestmysql';
GRANT ALL ON `hapiestmysql`.* to 'hapiestmysql'@'127.0.0.1' IDENTIFIED BY 'hapiestmysql';

FLUSH PRIVILEGES;