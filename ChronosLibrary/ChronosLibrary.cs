//Copyright - Martin Auterský from Hidden Valor team

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Transactions;

/// <summary>
/// Class for database handling
/// </summary>
public class ChronosLibrary {
    private readonly string _provider;
    private readonly string _connString;
    private readonly List<string> _commandList;

    /// <summary>
    /// Constructor - initializes provider, connection string and command list for transactions
    /// </summary>
    /// <param name="providerName">Provider</param>
    /// <param name="connString">Connection string</param>
    public ChronosLibrary(string providerName, string connString) {
        _provider = providerName;
        _connString = connString;
        _commandList = new List<string>();
    }

    /// <summary>
    /// Method for connection testing - throws exception - handled in app
    /// </summary>
    public void TestConnection() {
        var factory = DbProviderFactories.GetFactory(_provider);
        using (var conn = factory.CreateConnection()) {
            conn.ConnectionString = _connString;
            conn.Open();
        }
    }

    /// <summary>
    /// Method for clearing the command list
    /// </summary>
    public void ClearTransaction() {
        _commandList.Clear();
    }

    /// <summary>
    /// Method for executing the transaction
    /// </summary>
    public void ExecuteTransaction() {
        using (var scope = new TransactionScope()) {                            //Using transaction scope - every command executed withing the scope has to be successful
            var factory = DbProviderFactories.GetFactory(_provider);            //Factory creation
            var conn = factory.CreateConnection();                              //Connection creation
            conn.ConnectionString = _connString;

            using (conn) {                                                      //Using created connection
                conn.Open();
                foreach (var command in _commandList) {                         //Iterating through every saved command/query
                    var dbCommand = conn.CreateCommand();                       //Command creation
                    dbCommand.CommandText = command;
                    dbCommand.CommandType = CommandType.Text;
                    dbCommand.ExecuteNonQuery();                                //Command execution
                }
            }
            scope.Complete();                                                   //Commiting transaction
        }
    }

    /// <summary>
    /// Method for value returning operations
    /// </summary>
    /// <param name="queryString">Database query</param>
    /// <returns>Data table filled with values</returns>
    private DataTable ExecuteFunctionWithResult(string queryString) {
        var factory = DbProviderFactories.GetFactory(_provider);                //Factory creation
        var conn = factory.CreateConnection();                                  //Connection creation
        conn.ConnectionString = _connString;

        using (conn) {                                                          //Using created connection
            var command = conn.CreateCommand();                                 //Command creation
            command.CommandText = queryString;
            command.CommandType = CommandType.Text;

            var adapter = factory.CreateDataAdapter();                          //Adapter creation
            adapter.SelectCommand = command;                                    //Using created command in adapter

            var table = new DataTable();
            adapter.Fill(table);                                                //Filling data table via created adapter
            return table;
        }
    }

    /// <summary>
    /// Method for non query operations
    /// </summary>
    /// <param name="commandString">Database command</param>
    private void ExecuteFunction(string commandString) {
        var factory = DbProviderFactories.GetFactory(_provider);                //Factory creation
        var conn = factory.CreateConnection();                                  //Connection creation
        conn.ConnectionString = _connString;

        using (conn) {                                                          //Using created connection
            conn.Open();
            var dbCommand = conn.CreateCommand();                               //Command creation
            dbCommand.CommandText = commandString;
            dbCommand.CommandType = CommandType.Text;
            dbCommand.ExecuteNonQuery();                                        //Command execution
        }
    }

    /// <summary>
    /// Method for extracting one value from data table
    /// </summary>
    /// <param name="table">Source table</param>
    /// <returns>Value string</returns>
    private static string ExtractFirst(DataTable table) {
        var dataRow = table.Rows[0];
        var dataColumn = table.Columns[0];
        return dataRow[dataColumn].ToString();
    }

    /// <summary>
    /// Method for adding set attribute operation to command list
    /// </summary>
    /// <param name="docId">Document ID</param>
    /// <param name="name">Attribute name</param>
    /// <param name="value">Attribute value</param>
    /// <param name="link">Link flag</param>
    /// <param name="user">User</param>
    /// <param name="pwd">User's password</param>
    public void AddSetAttribute(int docId, string name, string value, bool link, string user, string pwd) {
        _commandList.Add($"SELECT set_attr({docId}, '{name}', '{value}', {link}, uid('{user}'), '{pwd}');");
    }

    /// <summary>
    /// Method for adding reset attribute operation to command list
    /// </summary>
    /// <param name="docId">Document ID</param>
    /// <param name="name">Attribute name</param>
    /// <param name="user">User</param>
    /// <param name="pwd">User's password</param>
    public void AddResetAttribute(int docId, string name, string user, string pwd) {
        _commandList.Add($"SELECT reset_attr({docId}, '{name}', uid('{user}'), '{pwd}');");
    }

    /// <summary>
    /// Method for adding insert attribute operation to command list
    /// </summary>
    /// <param name="docId">Document ID</param>
    /// <param name="name">Attribute name</param>
    /// <param name="value">Attribute value</param>
    /// <param name="link">Link flag</param>
    /// <param name="user">User</param>
    /// <param name="pwd">User's password</param>
    public void AddInsertAttribute(int docId, string name, string value, bool link, string user, string pwd) {
        _commandList.Add($"SELECT insert_attr({docId}, '{name}', '{value}', {link}, uid('{user}'), '{pwd}');");
    }

    /// <summary>
    /// Method for adding remove attribute operation to command list
    /// </summary>
    /// <param name="docId">Document ID</param>
    /// <param name="name">Attribute name</param>
    /// <param name="value">Attribute value</param>
    /// <param name="user">User</param>
    /// <param name="pwd">User's password</param>
    public void AddRemoveAttribute(int docId, string name, string value, string user, string pwd) {
        _commandList.Add($"SELECT remove_attr({docId}, '{name}', '{value}', uid('{user}'), '{pwd}');");
    }

    /// <summary>
    /// Method for adding remove document function to command list
    /// </summary>
    /// <param name="docId">Document ID</param>
    /// <param name="user">Creator</param>
    /// <param name="pwd">Creator's password</param>
    public void AddRemoveDoc(int docId, string user, string pwd) {
        _commandList.Add($"SELECT remove_doc({docId}, uid('{user}'), '{pwd}')");
    }

    /// <summary>
    /// Method for attribute and value extraction
    /// </summary>
    /// <param name="docId">Document ID</param>
    /// <param name="time">Time stamp</param>
    /// <returns>Data table with retrieved information</returns>
    public DataTable ScanDocs(int docId, DateTime time) {
        var queryString = $"SELECT * FROM scandocs({docId}, '{time:yyyy-MM-dd HH:mm:ss.ffffff}');";
        return ExecuteFunctionWithResult(queryString);
    }

    /// <summary>
    /// Method for obtaining user's document list
    /// </summary>
    /// <param name="user">Username</param>
    /// <param name="pwd">User's password</param>
    /// <returns>Data table with retrieved information</returns>
    public DataTable ListDocs(string user, string pwd) {
        var queryString = $"SELECT * FROM list_docs(uid('{user}'), '{pwd}');";
        return ExecuteFunctionWithResult(queryString);
    }

    /// <summary>
    /// Method for retrieving lessee list
    /// </summary>
    /// <param name="docId">Document ID</param>
    /// <param name="user">Username</param>
    /// <param name="pwd">User's password</param>
    /// <returns>Data table with retrieved information</returns>
    public DataTable ListLessees(int docId, string user, string pwd) {
        var queryString = $"SELECT * FROM list_lessees({docId}, uid('{user}', '{pwd}');";
        return ExecuteFunctionWithResult(queryString);
    }

    /// <summary>
    /// Method for creating new document
    /// </summary>
    /// <param name="creator">Document creator</param>
    /// <param name="pwd">Creator's password</param>
    /// <returns>Document ID</returns>
    public int CreateDoc(string creator, string pwd) {
        var queryString = $"SELECT create_doc(uid('{creator}'), '{pwd}');";
        return int.Parse(ExtractFirst(ExecuteFunctionWithResult(queryString)));
    }

    /// <summary>
    /// Method for set attribute operation
    /// </summary>
    /// <param name="docId">Document ID</param>
    /// <param name="name">Attribute name</param>
    /// <param name="value">Attribute value</param>
    /// <param name="link">Link flag</param>
    /// <param name="user">Username</param>
    /// <param name="pwd">User's password</param>
    public void SetAttribute(int docId, string name, string value, bool link, string user, string pwd) {
        var queryString = $"SELECT set_attr({docId}, '{name}', '{value}', {link}, uid('{user}'), '{pwd}');";
        ExecuteFunction(queryString);
    }

    /// <summary>
    /// Method for reset attribute operation
    /// </summary>
    /// <param name="docId">Document ID</param>
    /// <param name="name">Attribute name</param>
    /// <param name="user">Username</param>
    /// <param name="pwd">User's password</param>
    public void ResetAttribute(int docId, string name, string user, string pwd) {
        var queryString = $"SELECT reset_attr({docId}, '{name}', uid('{user}'), '{pwd}');";
        ExecuteFunction(queryString);
    }

    /// <summary>
    /// Method for insert attribute to container operation
    /// </summary>
    /// <param name="docId">Document ID</param>
    /// <param name="name">Attribute name</param>
    /// <param name="value">Attribute value</param>
    /// <param name="link">Link flag</param>
    /// <param name="user">Username</param>
    /// <param name="pwd">User's password</param>
    public void InsertAttribute(int docId, string name, string value, bool link, string user, string pwd) {
        var queryString = $"SELECT insert_attr({docId}, '{name}', '{value}', {link}, uid('{user}'), '{pwd}');";
        ExecuteFunction(queryString);
    }

    /// <summary>
    /// Method for remove attribute from container operation
    /// </summary>
    /// <param name="docId">Document ID</param>
    /// <param name="name">Attribute name</param>
    /// <param name="value">Attribute value</param>
    /// <param name="user">Username</param>
    /// <param name="pwd">User's password</param>
    public void RemoveAttribute(int docId, string name, string value, string user, string pwd) {
        var queryString = $"SELECT remove_attr({docId}, '{name}', '{value}', uid('{user}'), '{pwd}');";
        ExecuteFunction(queryString);
    }

    /// <summary>
    /// Method for lease creation
    /// </summary>
    /// <param name="docId">Document ID</param>
    /// <param name="lessor">Lessor</param>
    /// <param name="pwd">Lessor's password</param>
    /// <param name="lessee">Lessee</param>
    public void CreateLease(int docId, string lessor, string pwd, string lessee) {
        var queryString = $"SELECT lease({docId}, uid('{lessor}'), '{pwd}', uid('{lessee}'));";
        ExecuteFunction(queryString);
    }

    /// <summary>
    /// Method for user creation
    /// </summary>
    /// <param name="user">Username</param>
    /// <param name="pwd">User's password</param>
    /// <param name="admin">Admin flag</param>
    /// <param name="creator">Creator</param>
    /// <param name="creatorPwd">Creator's password</param>
    /// <returns>New user ID</returns>
    public int CreateUser(string user, string pwd, bool admin, string creator, string creatorPwd) {
        var queryString = $"SELECT create_user('{user}', '{pwd}', {admin}, uid('{creator}'), '{creatorPwd}');";
        return int.Parse(ExtractFirst(ExecuteFunctionWithResult(queryString)));
    }

    /// <summary>
    /// Method for credentials check
    /// </summary>
    /// <param name="user">Username</param>
    /// <param name="pwd">User's password</param>
    public void Credentials(string user, string pwd) {
        var queryString = $"SELECT credentials(uid('{user}'), '{pwd}');";
        ExecuteFunction(queryString);
    }

    /// <summary>
    /// Method for admin privileges check
    /// </summary>
    /// <param name="user">Username</param>
    /// <returns>True if admin, false if not admin</returns>
    public bool IsAdmin(string user) {
        var queryString = $"SELECT isAdmin(uid('{user}'));";
        return bool.Parse(ExtractFirst(ExecuteFunctionWithResult(queryString)));
    }

    /// <summary>
    /// Method for document creator check
    /// </summary>
    /// <param name="docId">Document ID</param>
    /// <param name="user">Username</param>
    /// <returns>True if creator, false if not creator</returns>
    public bool IsCreator(int docId, string user) {
        var queryString = $"SELECT isCreator({docId}, uid('{user}'));";
        return bool.Parse(ExtractFirst(ExecuteFunctionWithResult(queryString)));
    }

    /// <summary>
    /// Method for obtaining user list
    /// </summary>
    /// <param name="user">Username</param>
    /// <param name="pwd">User's password</param>
    /// <returns>Data table with retrieved information</returns>
    public DataTable ListUsers(string user, string pwd) {
        var queryString = $"SELECT * FROM list_users(uid('{user}'), '{pwd}');";
        return ExecuteFunctionWithResult(queryString);
    }

    /// <summary>
    /// Method for obtaining document list
    /// </summary>
    /// <param name="user">Username</param>
    /// <param name="pwd">User's password</param>
    /// <returns>Data table with retrieved information</returns>
    public DataTable ListAllDocs(string user, string pwd) {
        var queryString = $"SELECT * FROM list_all_docs(uid('{user}'), '{pwd}');";
        return ExecuteFunctionWithResult(queryString);
    }

    /// <summary>
    /// Method for obtaining document scheme
    /// </summary>
    /// <param name="docId">Document ID</param>
    /// <param name="user">Username</param>
    /// <param name="pwd">User's password</param>
    /// <returns>Scheme ID</returns>
    public int GetSchemeId(int docId, string user, string pwd) {
        var queryString = $"SELECT get_scheme_id({docId}, uid('{user}'), '{pwd}');";
        return int.Parse(ExtractFirst(ExecuteFunctionWithResult(queryString)));
    }

    /// <summary>
    /// Method for obtaining document name
    /// </summary>
    /// <param name="docId">Document ID</param>
    /// <param name="user">Username</param>
    /// <param name="pwd">User's password</param>
    /// <returns>Document name</returns>
    public string GetName(int docId, string user, string pwd) {
        var queryString = $"SELECT get_name({docId}, uid('{user}'), '{pwd}');";
        return ExtractFirst(ExecuteFunctionWithResult(queryString));
    }

    /// <summary>
    /// Method for shadow document check
    /// </summary>
    /// <param name="docId">Document ID</param>
    /// <param name="user">Username</param>
    /// <param name="pwd">User's password</param>
    /// <returns>True if has shadow, false if not</returns>
    public bool HasShadow(int docId, string user, string pwd) {
        var queryString = $"SELECT has_shadow({docId}, uid('{user}'), '{pwd}');";
        return bool.Parse(ExtractFirst(ExecuteFunctionWithResult(queryString)));
    }
}
