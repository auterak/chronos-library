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
    /// <param name="user">Creator</param>
    /// <param name="pwd">Creator's password</param>
    public void AddSetAttribute(int docId, string name, string value, bool link, string user, string pwd) {
        _commandList.Add($"SELECT set_attr({docId}, '{name}', '{value}', {link}, uid('{user}'), '{pwd}');");
    }

    /// <summary>
    /// Method for adding reset attribute operation to command list
    /// </summary>
    /// <param name="docId">Document ID</param>
    /// <param name="name">Attribute name</param>
    /// <param name="user">Creator</param>
    /// <param name="pwd">Creator's password</param>
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
    /// <param name="user">Creator</param>
    /// <param name="pwd">Creator's password</param>
    public void AddInsertAttribute(int docId, string name, string value, bool link, string user, string pwd) {
        _commandList.Add($"SELECT insert_attr({docId}, '{name}', '{value}', {link}, uid('{user}'), '{pwd}');");
    }

    /// <summary>
    /// Method for adding remove attribute operation to command list
    /// </summary>
    /// <param name="docId">Document ID</param>
    /// <param name="name">Attribute name</param>
    /// <param name="value">Attribute value</param>
    /// <param name="user">Creator</param>
    /// <param name="pwd">Creator's password</param>
    public void AddRemoveAttribute(int docId, string name, string value, string user, string pwd) {
        _commandList.Add($"SELECT remove_attr({docId}, '{name}', '{value}', uid('{user}'), '{pwd}');");
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
    /// Method for retrieving user's document list
    /// </summary>
    /// <param name="user">Username</param>
    /// <param name="pwd">User's password</param>
    /// <returns>Data table with retrieved information</returns>
    public DataTable ListDocs(string user, string pwd) {
        var queryString = $"SELECT * FROM list_docs(uid('{user}'), '{pwd}');";
        return ExecuteFunctionWithResult(queryString);
    }

    /// <summary>
    /// Metoda pro získání seznamu nájemníků vybraného dokumentu
    /// </summary>
    /// <param name="docId">Identifikátor dokumentu</param>
    /// <param name="user">Jméno uživatele</param>
    /// <param name="pwd">Heslo uživatele</param>
    /// <returns>Datová tabulka se získanými informacemi</returns>
    public DataTable ListLessees(int docId, string user, string pwd) {
        var queryString = $"SELECT * FROM list_lessees({docId}, uid('{user}', '{pwd}');";
        return ExecuteFunctionWithResult(queryString);
    }

    /// <summary>
    /// Metoda pro vložení nového dokumentu do databáze
    /// </summary>
    /// <param name="creator">Tvůrce dokumentu</param>
    /// <param name="pwd">Heslo tvůrce</param>
    /// <returns>Identifikátor dokumentu</returns>
    public int InsertDoc(string creator, string pwd) {
        var queryString = $"SELECT insert_doc(uid('{creator}'), '{pwd}');";
        return int.Parse(ExtractFirst(ExecuteFunctionWithResult(queryString)));
    }

    /// <summary>
    /// Metoda pro vložení nekontejnerového atributu do dokumentu
    /// </summary>
    /// <param name="docId">Identifikátor dokumentu</param>
    /// <param name="name">Název atributu</param>
    /// <param name="value">Hodnota atributu</param>
    /// <param name="link">Příznak odkazu</param>
    /// <param name="user">Jméno uživatele</param>
    /// <param name="pwd">Heslo uživatele</param>
    public void SetAttribute(int docId, string name, string value, bool link, string user, string pwd) {
        var queryString = $"SELECT set_attr({docId}, '{name}', '{value}', {link}, uid('{user}'), '{pwd}');";
        ExecuteFunction(queryString);
    }

    /// <summary>
    /// Metoda pro odstranění atributu
    /// </summary>
    /// <param name="docId">Identifikátor dokumentu</param>
    /// <param name="name">Název atributu</param>
    /// <param name="user">Jméno uživatele</param>
    /// <param name="pwd">Heslo uživatele</param>
    public void ResetAttribute(int docId, string name, string user, string pwd) {
        var queryString = $"SELECT reset_attr({docId}, '{name}', uid('{user}'), '{pwd}');";
        ExecuteFunction(queryString);
    }

    /// <summary>
    /// Metoda pro vložení atributu do pole v dokumentu
    /// </summary>
    /// <param name="docId">Identifikátor dokumentu</param>
    /// <param name="name">Název atributu</param>
    /// <param name="value">Hodnota atributu</param>
    /// <param name="link">Příznak odkazu</param>
    /// <param name="user">Jméno uživatele</param>
    /// <param name="pwd">Heslo uživatele</param>
    public void InsertAttribute(int docId, string name, string value, bool link, string user, string pwd) {
        var queryString = $"SELECT insert_attr({docId}, '{name}', '{value}', {link}, uid('{user}'), '{pwd}');";
        ExecuteFunction(queryString);
    }

    /// <summary>
    /// Metoda pro odstranění atributu z pole
    /// </summary>
    /// <param name="docId">Identifikátor dokumentu</param>
    /// <param name="name">Název atributu</param>
    /// <param name="value">Hodnota atributu</param>
    /// <param name="user">Jméno uživatele</param>
    /// <param name="pwd">Heslo uživatele</param>
    public void RemoveAttribute(int docId, string name, string value, string user, string pwd) {
        var queryString = $"SELECT remove_attr({docId}, '{name}', '{value}', uid('{user}'), '{pwd}');";
        ExecuteFunction(queryString);
    }

    /// <summary>
    /// Metoda pro propůjčení dokumentu
    /// </summary>
    /// <param name="docId">Identifikátor dokumentu</param>
    /// <param name="lessor">Pronajímatel</param>
    /// <param name="pwd">Heslo pronajímatele</param>
    /// <param name="lessee">Nájemník</param>
    public void CreateLease(int docId, string lessor, string pwd, string lessee) {
        var queryString = $"SELECT lease({docId}, uid('{lessor}'), '{pwd}', uid('{lessee}'));";
        ExecuteFunction(queryString);
    }

    /// <summary>
    /// Metoda pro vytvoření nového uživatele
    /// </summary>
    /// <param name="user">Jméno uživatele</param>
    /// <param name="pwd">Heslo uživatele</param>
    /// <param name="admin">Příznak admina</param>
    /// <param name="creator">Tvůrce</param>
    /// <param name="creatorPwd">Heslo tvůrce</param>
    /// <returns>Identifikátor vytvořeného uživatele</returns>
    public int CreateUser(string user, string pwd, bool admin, string creator, string creatorPwd) {
        var queryString = $"SELECT create_user('{user}', '{pwd}', {admin}, uid('{creator}'), '{creatorPwd}');";
        return int.Parse(ExtractFirst(ExecuteFunctionWithResult(queryString)));
    }

    /// <summary>
    /// Metoda pro otestování údajů uživatele
    /// </summary>
    /// <param name="user">Jméno uživatele</param>
    /// <param name="pwd">Heslo uživatele</param>
    public void Credentials(string user, string pwd) {
        var queryString = $"SELECT credentials(uid('{user}'), '{pwd}');";
        ExecuteFunction(queryString);
    }

    /// <summary>
    /// Metoda pro otestování, zda je uživatel adminem
    /// </summary>
    /// <param name="user">Jméno uživatele</param>
    /// <returns>True pokud je adminem, false pokud není adminem</returns>
    public bool IsAdmin(string user) {
        var queryString = $"SELECT isAdmin(uid('{user}'));";
        return bool.Parse(ExtractFirst(ExecuteFunctionWithResult(queryString)));
    }

    /// <summary>
    /// Metoda pro otestování, zda je uživatel tvůrcem dokumentu
    /// </summary>
    /// <param name="docId">Identifikátor dokumentu</param>
    /// <param name="user">Jméno uživatele</param>
    /// <returns>True pokud je tvůrce, false pokud není tvůrcem</returns>
    public bool IsCreator(int docId, string user) {
        var queryString = $"SELECT isCreator({docId}, uid('{user}'));";
        return bool.Parse(ExtractFirst(ExecuteFunctionWithResult(queryString)));
    }

    /// <summary>
    /// Metoda pro získání seznamu všech uživatelů
    /// </summary>
    /// <param name="user">Jméno uživatele</param>
    /// <param name="pwd">Heslo uživatele</param>
    /// <returns>Datová tabulka se získanými informacemi</returns>
    public DataTable ListUsers(string user, string pwd) {
        var queryString = $"SELECT * FROM list_users(uid('{user}'), '{pwd}');";
        return ExecuteFunctionWithResult(queryString);
    }

    /// <summary>
    /// Metoda pro získání seznamu všech dokumentů
    /// </summary>
    /// <param name="user">Jméno uživatele</param>
    /// <param name="pwd">Heslo uživatele</param>
    /// <returns>Datová tabulka se získanými informacemi</returns>
    public DataTable ListAllDocs(string user, string pwd) {
        var queryString = $"SELECT * FROM list_all_docs(uid('{user}'), '{pwd}');";
        return ExecuteFunctionWithResult(queryString);
    }

    /// <summary>
    /// Metoda pro získání identifikátoru schématu dokumentu
    /// </summary>
    /// <param name="docId">Identifikátor dokumentu</param>
    /// <param name="user">Jméno uživatele</param>
    /// <param name="pwd">Heslo uživatele</param>
    /// <returns>Identifikátor schématu</returns>
    public int GetSchemeId(int docId, string user, string pwd) {
        var queryString = $"SELECT get_scheme_id({docId}, uid('{user}'), '{pwd}');";
        return int.Parse(ExtractFirst(ExecuteFunctionWithResult(queryString)));
    }

    /// <summary>
    /// Metoda pro získání názvu dokumentu
    /// </summary>
    /// <param name="docId">Identifikátor dokumentu</param>
    /// <param name="user">Jméno uživatele</param>
    /// <param name="pwd">Heslo uživatele</param>
    /// <returns>Název dokumentu</returns>
    public string GetName(int docId, string user, string pwd) {
        var queryString = $"SELECT get_name({docId}, uid('{user}'), '{pwd}');";
        return ExtractFirst(ExecuteFunctionWithResult(queryString));
    }

    /// <summary>
    /// Metoda pro zjištění odvození dokumentu
    /// </summary>
    /// <param name="docId">Identifikátor dokumentu</param>
    /// <param name="user">Jméno uživatele</param>
    /// <param name="pwd">Heslo uživatele</param>
    /// <returns>True pokud je odvozen, false pokud není odvozen</returns>
    public bool HasShadow(int docId, string user, string pwd) {
        var queryString = $"SELECT has_shadow({docId}, uid('{user}'), '{pwd}');";
        return bool.Parse(ExtractFirst(ExecuteFunctionWithResult(queryString)));
    }
}
