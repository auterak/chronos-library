using System;
using System.Data;
using System.Data.Common;

/// <summary>
/// Třída pro propojení aplikace s databází
/// </summary>
public class ChronosLibrary {
    private readonly string _provider;
    private readonly string _connString;

    /// <summary>
    /// Konstruktor, nastavuje poskytovatele a řetězec pro připojení
    /// </summary>
    /// <param name="providerName">Poskytovatel</param>
    /// <param name="connString">Řetězec pro připojení</param>
    public ChronosLibrary(string providerName, string connString) {
        _provider = providerName;
        _connString = connString;
    }

    /// <summary>
    /// Metoda pro testování spojení - pokud nelze navázat spojení, nastane vyjímka
    /// </summary>
    public void TestConnection() {
        var factory = DbProviderFactories.GetFactory(_provider);
        using (var conn = factory.CreateConnection()) {
            conn.ConnectionString = _connString;
            conn.Open();
        }
    }

    /// <summary>
    /// Metoda pro operace, které vrací hodnotu
    /// </summary>
    /// <param name="queryString">Dotaz do databáze</param>
    /// <returns>Datová tabulka s hodnotami</returns>
    private DataTable ExecuteFunctionWithResult(string queryString) {
        //Vytvoření továrny
        var factory = DbProviderFactories.GetFactory(_provider);
        //Vytvoření spojení
        var conn = factory.CreateConnection();
        conn.ConnectionString = _connString;
        //Použití vytvořeného spojení
        using (conn) {
            //Vytvoření příkazu
            var command = conn.CreateCommand();
            command.CommandText = queryString;
            command.CommandType = CommandType.Text;
            //Vytvoření adaptéru
            var adapter = factory.CreateDataAdapter();
            adapter.SelectCommand = command;
            //Vytvoření a naplnění datové tabulky
            var table = new DataTable();
            adapter.Fill(table);
            return table;
        }
    }

    /// <summary>
    /// Metoda pro operace, které nevracejí hodnotu
    /// </summary>
    /// <param name="queryString">Dotaz do databáze</param>
    private void ExecuteFunction(string queryString) {
        //Vytvoření tovární
        var factory = DbProviderFactories.GetFactory(_provider);
        //Vytvoření připojení
        var conn = factory.CreateConnection();
        conn.ConnectionString = _connString;
        //Použití připojení
        using (conn) {
            conn.Open();
            //Vytvoření příkazu
            var dbCommand = conn.CreateCommand();
            dbCommand.CommandText = queryString;
            dbCommand.CommandType = CommandType.Text;
            //Vykonání příkazu
            dbCommand.ExecuteNonQuery();
        }
    }

    /// <summary>
    /// Metoda pro získání jedné hodnoty z datové tabulky
    /// </summary>
    /// <param name="table">Zdrojová tabulka</param>
    /// <returns>Řetězec s hodnotou</returns>
    private static string ExtractFirst(DataTable table) {
        var dataRow = table.Rows[0];
        var dataColumn = table.Columns[0];
        return dataRow[dataColumn].ToString();
    }

    /// <summary>
    /// Metoda pro získání seznamu atributů a hodnot z dokumentu
    /// </summary>
    /// <param name="docId">Identifikátor dokumentu</param>
    /// <param name="time">Časová značka</param>
    /// <returns>Datová tabulka se získanými informacemi</returns>
    public DataTable ScanDocs(int docId, DateTime time) {
        var queryString = $"SELECT * FROM scandocs({docId}, '{time:yyyy-MM-dd HH:mm:ss.ffffff}');";
        return ExecuteFunctionWithResult(queryString);
    }

    /// <summary>
    /// Metoda pro získání seznamu dokumentů vybraného uživatele
    /// </summary>
    /// <param name="user">Jméno uživatele</param>
    /// <param name="pwd">Heslo uživatele</param>
    /// <returns>Datová tabulka se získanými informacemi</returns>
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
