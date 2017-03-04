# Hapiest-Mysql
A wrapper around mysql that provides a very descriptive way of running queries.

# Main Components

+ MysqlDao: a wrapper to allow interaction with a specific MySQL table
+ MysqlService: base wrapper around passing insert/update/delete/select statements to MySQL. Supports multiple read connections.

# How to use MysqlDao

First, you'll need to instantiate an instance of MysqlService to inject to the DAO, which you can do via the MysqlServiceSingleton.
Hapiest-mysql is designed to work with [node-config|https://github.com/lorenwest/node-config] out of the box.
 It's not necessary but we recommend it as a best practice.
 
 We also recommend that you use hapiest-logger though any logger that has an error(msg, dataObj) function suffices.
 
 Next, you'll want to declare a class that extends from MysqlDao and provides the table name.
 
 You'll then may need to define a function that maps raw DB results to results you want.  
 For instance, we tend to convert the raw results into an immutable Value Object (VO) to encourage best practices. 

```javascript
const MysqlDao = require('hapiest-mysql/lib/mysqlDao');
const mysqlService = require('hapiest-mysql/lib/mysqlServiceSington').getMysqlService();

class UsersDao extends MysqlDao {
    // Must override tableName getter
    get tableName() { return 'users'; }
}

// Assume the users table has columns id, email, and first_name
// Define the immutable UsersVO object
const VO = require('hapiest-vo');
class UsersVO extends VO {
    constructor(config) {
        super();
        this._addProperties(config);
    }
    
    get id() {return this.get('id');}
    get email() {return this.get('email');}
    get firstName() {return this.get('firstName');}
}

// Define the factory class for the UsersVO, which provides a function to create a user from a row in the database
class UsersVOFactory {   
    static createFromDbRow(dbRow) {
        return new UsersVO({
            id: dbRow.id,
            email: dbRow.email,
            firstName: dbRow.first_name
        });
    }
}

const logger = require('hapiest-logger/lib/loggerSingleton').getLogger();

// Instantiate your DAO
const userDao = new UsersDao({
    mysqlService: mysqlService,
    createVoFromDbRowFunction: UsersVOFactory.createFromDbRow,
    logger: logger
});

userDao.create({email: 'john.doe@gmail.com', firstName: 'john'})
.then(id => userDao.getOneById(id))
.then(userVO => {
    // Do something with the user object
});

```