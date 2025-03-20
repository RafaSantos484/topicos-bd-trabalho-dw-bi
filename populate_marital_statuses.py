import json
import oracledb


def _load_json(filename: str):
    with open(filename, "r", encoding="utf-8") as file:
        return json.load(file)


def _run_sql_ignoring_err(cursor: oracledb.Cursor, script: str):
    try:
        cursor.execute(script)
    except:
        pass


def run(cursor: oracledb.Cursor):
    _run_sql_ignoring_err(
        cursor, "ALTER TABLE c##dw_oltp.cliente ADD estado_civil CHAR NULL")
    _run_sql_ignoring_err(
        cursor, "UPDATE c##dw_oltp.cliente SET estado_civil = 'S'")

    clients_complementar_data = _load_json("Clientes_Dados_Complementar.json")
    clients: list[dict] = clients_complementar_data["Cliente"]
    clients_tp = [(client["Estado_civil"], client["ID"]) for client in clients]
    cursor.executemany(
        "UPDATE c##dw_oltp.cliente SET estado_civil = :1 WHERE id_cliente = :2", clients_tp)
    print("Inserted marital statuses from Clientes_Dados_Complementar.json into 'c##dw_oltp.cliente' table")
