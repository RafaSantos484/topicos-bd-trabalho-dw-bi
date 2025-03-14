import json
import getpass
import oracledb
import pandas as pd


def load_json(filename: str):
    with open(filename, "r", encoding="utf-8") as file:
        return json.load(file)


def run_sql_script(cursor: oracledb.Cursor, script_path: str):
    with open(script_path, "r") as file:
        script = file.read()
    for command in script.split(";"):
        try:
            cursor.execute(command)
        except:
            pass

    print(f"executed {script_path}")


def get_time_id(cursor: oracledb.Cursor, year: int, quarter: str) -> int:
    cursor.execute(
        """
        SELECT
            id_tempo
        FROM
            c##dw_olap.tempo
        WHERE
                ano = :1
            AND quadrimestre = :2
        """,
        (year, quarter)
    )
    result = cursor.fetchone()
    if result is None:
        id = cursor.var(int)
        cursor.execute(
            """
            INSERT INTO c##dw_olap.tempo (
                ano,
                quadrimestre
            ) VALUES ( :1, :2 ) RETURNING id_tempo INTO : 3
            """,
            (year, quarter, id)
        )
        return id.getvalue()[0]
    else:
        return result[0]


def populate_olap_db(connection: oracledb.Connection) -> None:
    cursor = connection.cursor()

    run_sql_script(cursor, "clear_olap_db.sql")  # TEMP
    run_sql_script(cursor, "fix_cidades_duplicadas.sql")
    run_sql_script(cursor, "fix_nomeclatura_sexo.sql")

    clients_complementar_data = load_json("Clientes_Dados_Complementar.json")
    clients: list[dict] = clients_complementar_data["Cliente"]
    cursor.executemany(
        """
        INSERT INTO c##dw_olap.cliente (
            id_cliente,
            estado_civil
        ) VALUES ( :1, :2 )
        """,
        [(client["ID"], client["Estado_civil"]) for client in clients]
    )
    print("Populated 'clientes' table from Clientes_Dados_Complementar.json")

    run_sql_script(cursor, "populate_olap_db.sql")

    competitors_sheet = pd.read_excel("Concorrente.xls")
    competitors_sheet["Quadrimestre"] = (
        ((competitors_sheet['Mês'] - 1) // 4) + 1).astype(str)
    competitors_sheet = competitors_sheet.groupby(['Ano', 'Quadrimestre']).agg(
        {'Valor': 'sum', 'Quantidade': 'sum'}).reset_index()
    competitors_sheet["time_id"] = competitors_sheet.apply(
        lambda row: get_time_id(cursor, row["Ano"], row["Quadrimestre"]), axis=1)
    cursor.executemany(
        """
        INSERT INTO c##dw_olap.concorrente (
            id_tempo,
            quantidade_concorrente,
            quantidade,
            valor_concorrente,
            valor
        )
            SELECT
                id_tempo,
                :1,
                sum_quantidade,
                :2,
                sum_valor
            FROM
                (
                    SELECT
                        id_tempo,
                        SUM(quantidade) sum_quantidade,
                        SUM(valor)      sum_valor
                    FROM
                        c##dw_olap.venda
                    GROUP BY
                        id_tempo
                )
            WHERE
                id_tempo = :3
        """,
        list(map(tuple,
                 competitors_sheet[["Quantidade", "Valor", "time_id"]].astype(str).values)
             )
    )
    print("Populated 'competitors' table from Concorrente.xls")

    cursor.close()


connection: oracledb.Connection = None
try:
    pw = getpass.getpass("Enter C##DW_OLAP password: ")
    connection = oracledb.connect(
        user="C##DW_OLAP",
        password=pw,
        dsn="localhost:1521/xe")
    print("Successfully connected to Oracle Database")

    populate_olap_db(connection)
    connection.commit()
except Exception as e:
    print(f"ERROR: {e}")
finally:
    if connection:
        connection.close()
