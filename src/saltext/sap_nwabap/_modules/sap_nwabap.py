"""
SaltStack extension for SAP NetWeaver AS ABAP
Copyright (C) 2022 SAP UCC Magdeburg

SAP NetWeaver AS ABAP execution module
======================================
SaltStack execution module for SAP NetWeaver AS ABAP.

:codeauthor:    Benjamin Wegener, Alexander Wilke
:maturity:      new
:depends:       pyrfc
:platform:      All

This module implements functions for SAP NetWeaver utilizing the SAP NetWeaver RFC SDK and the
python wrapper ``pyrfc``.
"""
import logging
import pprint
from datetime import datetime

# from urllib3.exceptions import NewConnectionError

# Third Party libs
PYRFCLIB = True
try:
    from pyrfc import Connection
    from pyrfc._exception import LogonError, ABAPApplicationError
    from pyrfc import RFCError
except ImportError:
    PYRFCLIB = False

# Globals
log = logging.getLogger(__name__)

__virtualname__ = "sap_nwabap"


def __virtual__():
    """
    Only load this module if all libraries are available
    """
    if not PYRFCLIB:
        return False, "Could not load module, pyrfc unavailable"
    return __virtualname__


# pylint: disable=too-many-leading-hastag-for-block-comment
### HELPER FUNCTIONS ################################################################################################


def process_bapiret2(ret):
    """
    Processes the returned ``BAPIRET2`` table

    ret
        ``BAPIRET2`` return table

    CLI Example:

    .. code-block:: bash

        salt "*" sap_nwabap.process_bapiret2 ret='{...}'
    """
    # return is either a single dict or a list
    if isinstance(ret, dict):
        ret = [ret]
    for message in ret:
        num = message["NUMBER"]
        msg = message["MESSAGE"]
        if num == "000":
            log.debug("Transaction commit successful")
        elif num == "017":
            log.error("Default company address missing in system, cannot create user")
            # see https://launchpad.support.sap.com/#/notes/0002222171
            return False
        elif num == "029":
            log.error(f"{msg}")  # No changes made to user
            return False
        elif num == "039":
            log.debug(f"{msg}")  # User <username> has changed
        elif num == "041":
            log.debug(f"{msg}")  # Password for user changed but not set as a production password
        elif num == "046":
            log.debug(f"{msg}")  # Profile assignement changed
        elif num == "048":
            log.debug(f"{msg}")  # Role assignement changed
        elif num == "049":
            log.debug(f"{msg}")  # Role assignement executed successfully
        elif num == "057":
            log.debug(f"{msg}")  # No job was found that corresponds to the specification
        elif num == "089":
            log.debug(f"{msg}")  # Profile assignement deleted
        elif num == "090":
            log.debug(f"{msg}")  # Role assignment deleted
        elif num == "102":
            log.debug(f"{msg}")  # User created
        elif num == "124":
            log.debug(f"{msg}")  # User does not exist
            return False
        elif num == "216":
            log.error(f"{msg}")  # Role does not exist
            return False
        elif num == "224":
            log.error(f"{msg}")  # User already exists
            return False
        elif num == "232":
            log.debug(f"{msg}")  # User deleted
        elif num == "245":
            log.debug(f"{msg}")  # User locked
        elif num == "246":
            log.debug(f"{msg}")  # User unlocked
        elif num == "255":
            log.error(f"{msg}")  # Profile does not exist
            return False
        elif num == "290":
            log.error(f"{msg}")  # Enter an initial password
            return False
        else:
            log.error(f"Unknown error {num}: {msg}")
            return False
    return True


# pylint: disable=unused-argument
def call_fms(
    function_modules,
    sid=None,
    client=None,
    message_server_host=None,
    message_server_port=None,
    logon_group=None,
    username=None,
    password=None,
    conn=None,
    continue_on_error=False,
    raise_on_error=False,
    **kwargs,
):
    """
    Calls multiple function modules in sequence with the same connection.
    Required for calls like ``BAPI_USER_CHANGE`` -> ``BAPI_TRANSACTION_COMMIT``.

    ``function_modules`` should be a dictionary with:

    .. code-block:: python

        {
            "<function module>": {
                "<arg_name>": "<arg_value>",
                ...
            },
            ...
        }

    The function will return two values, execution success (``True``/``False``) and result of the
    function modules as a dict, if there are any.

    function_modules
        Function modules and parameters to execute.

    sid
        SID of the SAP system (optional if a connection is given).

    message_server_host
        Host of the message server (optional if a connection is given).

    message_server_port
        Port of the message server (optional if a connection is given).

    client
        Client to connect to (optional if a connection is given).

    logon_group
        Logon group to use (optional if a connection is given).

    username
        Username (optional if a connection is given).

    password
        Password (optional if a connection is given).

    conn
        Existing pyrfc connection to user (optional if the parameters above are given).

    continue_on_error
        Continue with the execution of the next function module (``True|False``), default is
        ``False``. Not evaluated if ``raise_on_error=True``.

    raise_on_error
        Raise an exception (``True|False``) when an error occurs, default is ``False``.

    CLI Example:

    .. code-block:: bash

        salt "*" sap_nwabap.call_fms function_modules='{"RFC_PING": {}}' sid="S4H" client="000" message_server_host="s4h" message_server_port="3600" logon_group="SPACE" username="DDIC" password="Abcd1234$"
    """  # pylint: disable=line-too-long

    def _exec(conn, function_modules, continue_on_error, raise_on_error):
        """
        Helper function to allow provided and generated connections
        """
        success = True
        results = {}
        for function_module, args in function_modules.items():
            log.trace(
                f"Running function module '{function_module}' with the following arguments:\n{pprint.pformat(args)}"
            )
            try:
                result = conn.call(function_module, **args)
            except LogonError as lerr:
                logging.error(f"Could not logon with username {username}:\n{lerr}")
                if raise_on_error:
                    raise
                else:
                    # on logon errors, always break (next FMs *will* fail)
                    success = False
                    break
            except RFCError as rerr:
                log.error(f"An exception occured:\n{rerr}")
                if raise_on_error:
                    raise
                elif continue_on_error:
                    success = False
                    continue
                else:
                    success = False
                    break
            except Exception as exc:  # pylint: disable=broad-except
                log.error(f"An exception occured:\n{exc}")
                if raise_on_error:
                    raise
                elif continue_on_error:
                    success = False
                    continue
                else:
                    success = False
                    break
            log.trace(
                f"Got result for function module '{function_module}':\n{pprint.pformat(result)}"
            )
            results[function_module] = result
        return success, results

    log.debug("Running function")
    log.trace(f"Running for function modules / arguments:\n{function_modules}")
    success = True
    results = {}
    if conn:
        success, results = _exec(conn, function_modules, continue_on_error, raise_on_error)
    else:
        if None in [
            sid,
            message_server_host,
            message_server_port,
            client,
            logon_group,
            username,
            password,
        ]:
            log.error("Required arguments are not provided")
            return False, results
        log.debug("Creating temporary connection for function modules")
        if isinstance(client, int):
            client = f"{client:03d}"
        abap_connection = {
            "mshost": message_server_host,
            "msserv": str(message_server_port),
            "sysid": sid,
            "group": logon_group,
            "client": client,
            "user": username,
            "passwd": password,
            "lang": "EN",
        }
        with Connection(**abap_connection) as conn:
            success, results = _exec(conn, function_modules, continue_on_error, raise_on_error)
    log.trace(f"Returning {success} with resultset:\n{results}")
    return success, results


# pylint: disable=too-many-leading-hastag-for-block-comment
### FUNCTION MODULES ################################################################################################


def ping(
    sid, message_server_host, message_server_port, client, logon_group, username, password, **kwargs
):
    """
    Calls the function module ``RFC_PING``.

    sid
        SID of the SAP system.

    message_server_host
        Host of the message server.

    message_server_port
        Port of the message server.

    client
        Client to connect to.

    logon_group
        Logon group to use.

    username
        Username to use for the connection.

    password
        Password to use for the connection.

    CLI Example:

    .. code-block:: bash

        salt "*" sap_nwabap.ping sid="S4H" client="000" message_server_host="s4h" message_server_port="3600" logon_group="SPACE" username="DDIC" password="Abcd1234$"
    """  # pylint: disable=line-too-long
    log.debug("Running function")
    data = {"RFC_PING": {}}
    success, result = call_fms(
        sid=sid,
        function_modules=data,
        client=client,
        message_server_host=message_server_host,
        message_server_port=message_server_port,
        logon_group=logon_group,
        username=username,
        password=password,
        **kwargs,
    )
    if not success:
        log.error(f"Could not execute RFC_PING for {sid}, please check logs")
        return False
    log.debug(f"Got result:\n{result}")
    # on success, result is empty
    return True


# pylint: disable=unused-argument
def read_table(
    table_name,
    fields=None,
    sid=None,
    client=None,
    message_server_host=None,
    message_server_port=None,
    logon_group=None,
    username=None,
    password=None,
    conn=None,
    delimiter=";",
    **kwargs,
):
    """
    Reads and returns an ABAP table as list of dicts.

    .. note::
        All read fields may only have a combined length of 512 characters. If you want to retrieve
        more data, you need to call this function multiple times. This is a restriction by the
        function modules ``RFC_GET_TABLE_ENTRIES`` and ``RFC_READ_TABLE``.

    table_name
        Name of the table to read.

    fields
        Fields that should be retrieved. All will be retrieved if no list is given.

    sid
        SID of the SAP system (optional if a connection is given).

    message_server_host
        Host of the message server (optional if a connection is given).

    message_server_port
        Port of the message server (optional if a connection is given).

    client
        Client to connect to (optional if a connection is given).

    logon_group
        Logon group to use (optional if a connection is given).

    username
        Username (optional if a connection is given).

    password
        Password (optional if a connection is given).

    conn
        Existing pyrfc connection to user (optional if the parameters above are given).

    delimiter
        Delimiter to use for the function module ``RFC_READ_TABLE``, default is ``;``.

    CLI Example:

    .. code-block:: bash

        salt "*" sap_nwabap.read_table table_name="SLDAGADM" fields='["PROGNAME", "ACTIVE", "SEQNR", "RFCDEST", "DORFC", "DOBTC", "BTCMIN"]' sid="S4H" client="000" message_server_host="s4h" message_server_port="3600" logon_group="SPACE" username="DDIC" password="Abcd1234$"
    """  # pylint: disable=line-too-long
    log.debug(f"Reading table {table_name}")

    function_modules = {"RFC_READ_TABLE": {"QUERY_TABLE": table_name, "DELIMITER": delimiter}}
    if fields:
        function_modules["RFC_READ_TABLE"]["FIELDS"] = []
        for field in fields:
            function_modules["RFC_READ_TABLE"]["FIELDS"].append({"FIELDNAME": field})
    success = False
    try:
        success, result = call_fms(
            function_modules=function_modules,
            conn=conn,
            sid=sid,
            client=client,
            message_server_host=message_server_host,
            message_server_port=message_server_port,
            logon_group=logon_group,
            username=username,
            password=password,
            raise_on_error=True,
        )
    except ABAPApplicationError as aae:
        if aae.key == "DATA_BUFFER_EXCEEDED":
            msg = (
                "Length of received data exceeds 512 characters, "
                "please reduce number of retrieved fields"
            )
            log.error(msg)
            success = False
    except Exception as exc:  # pylint: disable=broad-except
        log.exception(exc)
        success = False
    if not success:
        log.error(f"Could not read table {table_name}")
        return False
    log.debug("Parsing result")
    ret = []
    for row in result["RFC_READ_TABLE"]["DATA"]:
        ret_row = {}
        row_columns = row["WA"].split(";")
        for i in range(0, len(result["RFC_READ_TABLE"]["FIELDS"])):
            value = row_columns[i].strip()
            field_type = result["RFC_READ_TABLE"]["FIELDS"][i]["TYPE"]
            # cast data
            """
            Known datatypes:
                C	Zeichenfolge  (Character)
                N	Zeichenfolge nur mit Ziffern
                D	Datum (Date: JJJJMMTT)
                T	Zeitpunkt (Time: HHMMSS)
                X	Bytefolge (heXadecimal), in Ddic-Metadaten auch für INT1/2/4
                I	Ganze Zahl (4-Byte Integer mit Vorzeichen)
                b	1-Byte Integer, ganze Zahl <= 254
                s	2-Byte Integer, nur für Längenfeld vor LCHR oder LRAW
                F	Gleitpunktzahl (Float) mit 8 Byte Genauigkeit
                g	Zeichenfolge mit variabler Länge (ABAP-Typ STRING)
                y	Bytefolge mit variabler Länge (ABAP-Typ XSTRING)
                V	Zeichenfolge (alter Dictionary-Typ VARC)
                a	Dezimale Gleitpunktzahl, 16 Ziffern
                e	Dezimale Gleitpunktzahl, 34 Ziffern
            Unknown datatypes:
                P	Gepackte Zahl (Packed)
                u	Strukturierter Typ, flach
                v	Strukturierter Typ, tief
                h	Tabellentyp
                r	Referenz auf Klasse/Interface
                l	Referenz auf Datenobjekt
                j	Statische Boxed Components
                k	Generische Boxed Components
                z	Knotenzeile bei Strukturierten Objekten
                    8	Ganze Zahl (8-Byte Integer mit Vorzeichen)
            """  # pylint: disable=pointless-string-statement
            if field_type in ["C", "N", "V", "g", "y"]:
                # string
                pass
            elif field_type in ["X", "I", "b", "s", "8"]:
                # integer
                try:
                    value = int(value)
                except ValueError:
                    log.error(f"Cannot cast '{value}' of type '{field_type}' to int, skipping")
            elif field_type in ["F", "a", "e"]:
                # float
                try:
                    value = float(value)
                except ValueError:
                    log.error(f"Cannot cast '{value}' of type '{field_type}' to float, skipping")
            elif field_type in ["D"]:
                # date
                try:
                    value = datetime.strptime(value, "%Y%m%D").date()
                except ValueError:
                    log.error(f"Cannot cast '{value}' of type '{field_type}' to date, skipping")
            elif field_type in ["T"]:
                # time
                try:
                    value = datetime.strptime(f"99991231 {value}", "%Y%m%D %H%M%S").time()
                except ValueError:
                    log.error(f"Cannot cast '{value}' of type '{field_type}' to time, skipping")
            else:
                log.info(
                    f"Fields of type {field_type} are currently not casted and left as strings"
                )
            ret_row[result["RFC_READ_TABLE"]["FIELDS"][i]["FIELDNAME"]] = value
        ret.append(ret_row)
    log.debug(f"Returning:\n{ret}")
    return ret
