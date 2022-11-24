"""
SaltStack extension for SAP NetWeaver
Copyright (C) 2022 SAP UCC Magdeburg

SAP NetWeaver AS ABAP state module
==================================
SaltStack module that implements SAP NetWeaver states based on the SAP NetWeaver RFC SDK.

:codeauthor:    Benjamin Wegener, Alexander Wilke
:maturity:      new
:depends:       pyrfc
:platform:      All

This module implements states for SAP NetWeaver utilizing the SAP NetWeaver RFC SDK and the
python wrapper ``pyrfc``. The states uses SAP function modules to read the current state
of the system and write new information back.

.. warning::
    Not all function modules are supported by SAP, meaning that the can be removed by SAP at any
    time or in case of errors, they might not be fixed.
"""
import logging
from datetime import date
from datetime import datetime

import salt.utils.dictdiffer
import salt.utils.dictupdate

# Third Party libs
PYRFCLIB = True
try:
    from pyrfc import Connection
    from pyrfc._exception import ABAPApplicationError
except ImportError:
    PYRFCLIB = False

# Globals
log = logging.getLogger(__name__)

__virtualname__ = "sap_nwabap"

# USER_MAPPING = {<human_readable_name>: <sap_key>}
USER_MAPPING = {
    "ACCOUNT_ID": "ACCNT",
    "ADDRESS_NUMBER": "ADDR_NO",
    "ADDRESS_DATA": "ADDRESS",
    "ADDRESS_NOTES": "ADR_NOTES",
    "USERNAME_ALIAS": "ALIAS",
    "CUA_REDISTRIBUTION": "BACK_DISTRIBUTION",
    "USERNAME": "BAPIBNAME",
    "CUA_REDISTRIBUTION_FLAG": "BAPIFLAG",
    "PWD_HASH_KEY": "BCODE",
    "BIRTH_NAME": "BIRTH_NAME",
    "CHARGABLE_USER": "BNAME_CHARGEABLE",
    "BUILDING_CODE_LONG": "BUILD_LONG",
    "BUILDING_CODE": "BUILDING",
    "BUILDING_CODE_P": "BUILDING_P",
    "CO_NAME": "C_O_NAME",
    "CATT_TEST_STATUS": "CATTKENNZ",
    "CITY_FILE_STATUS": "CHCKSTATUS",
    "CITY": "CITY",
    "CITY_CODE": "CITY_NO",
    "USER_GROUP": "CLASS",
    "CLIENT": "CLIENT",
    "PWD_HASH_CODE_VERSION_C": "CODVC",
    "PWD_HASH_VERSION": "CODVN",
    "PWD_HASH_CODE_VERSION_S": "CODVS",
    "COMMUNICATION_METHOD": "COMM_TYPE",
    "COMPANY": "COMPANY",
    "COMPANY_ADDRESS": "COMPANY",
    "COUNTRY_KEY": "COUNTRY",
    "COUNTRY_SURCHARGE": "COUNTRY_SURCHARGE",
    "COUNTRY_KEY_ISO": "COUNTRYISO",
    "COUNTY": "COUNTY",
    "COUNTY_CODE": "COUNTY_CODE",
    "DATE_FORMAT": "DATFM",
    "DECIMAL_FORMAT": "DCPFM",
    "USER_DEFAULTS": "DEFAULTS",
    "DELIVERY_SERVICE_NUMBER": "DELI_SERV_NUMBER",
    "DELIVERY_SERVICE_TYPE": "DELI_SERV_TYPE",
    "POST_DELIVERY_DISTRICT": "DELIV_DIS",
    "DEPARTMENT": "DEPARTMENT",
    "DESCRIPTION": "DESCRIPTION",
    "DISTRICT_CODE": "DISTRCT_NO",
    "DISTRUCT": "DISTRICT",
    "PO_BOX_ADDRESS": "DONT_USE_P",
    "STREET_ADDRESS": "DONT_USE_S",
    "EMAIL": "E_MAIL",
    "EXTERNAL_ID": "EXTID",
    "EXTERNAL_ID_CHANGE_INDICATOR": "EXTIDSX",
    "FAX_NUMBER_EXTENSION": "FAX_EXTENS",
    "FAX_NUMBER": "FAX_NUMBER",
    "FIRST_NAME": "FIRSTNAME",
    "BUILDING_FLOOR": "FLOOR",
    "BUILDING_FLOOR_P": "FLOOR_P",
    "FULL_NAME": "FULLNAME",
    "FULL_NAME_STATUS": "FULLNAME_X",
    "FUNCTION": "FUNCTION",
    "USER_VALID_TO": "GLTGB",
    "USER_VALID_FROM": "GLTGV",
    "SNC_ALLOW_PW_LOGON": "GUIFLAG",
    "HOME_CITY": "HOME_CITY",
    "HOME_CITY_CODE": "HOMECITYNO",
    "HOUSE_NUMBER": "HOUSE_NO",
    "HOUSE_NUMBER_SUPPLEMENT": "HOUSE_NO2",
    "HOUSE_NUMER_RANGE": "HOUSE_NO3",
    "POSTAL_CODE_INTERNAL": "INHOUSE_ML",
    "INITIALS": "INITIALS",
    "SHORT_NAME": "INITS_SIG",
    "COST_CENTER": "KOSTL",
    "LOGON_LANGUAGE": "LANGU",
    "LANGUAGE_KEY": "LANGU",
    "LANGUAGE_RECORD_CREATION": "LANGU_CR_P",
    "LANGUAGE_KEY_SAP": "LANGU_ISO",
    "LANGUAGE_KEY_P": "LANGU_P",
    "LANGUAGE_KEY_SAP_CP": "LANGUCPISO",
    "LANGUAGE_KEY_SAP_P": "LANGUP_ISO",
    "LAST_NAME": "LASTNAME",
    "LICENSE_TYPE": "LIC_TYPE",
    "LOCATION": "LOCATION",
    "LOGON_DATA": "LOGONDATA",
    "LAST_LOGON_TIME": "LTIME",
    "MIDDLE_NAME": "MIDDLENAME",
    "NAME_COUNTRY_FORMAT_RULE": "NAMCOUNTRY",
    "NAME_1": "NAME",
    "NAME_2": "NAME_2",
    "NAME_3": "NAME_3",
    "NAME_4": "NAME_4",
    "NAME_FORMAT": "NAMEFORMAT",
    "NICKNAME": "NICKNAME",
    "PWD_HASH_VALUE_SHA1": "PASSCODE",
    "PO_BOX_CITY_CODE": "PBOXCIT_NO",
    "POSTAL_CODE_EXTENSION_1": "PCODE1_EXT",
    "POSTAL_CODE_EXTENSION_2": "PCODE2_EXT",
    "POSTAL_CODE_EXTENSION_3": "PCODE3_EXT",
    "PERSON_NUMBER": "PERS_NO",
    "SNC_PRINTABLE_NAME": "PNAME",
    "PO_BOX": "PO_BOX",
    "PO_BOX_CITY": "PO_BOX_CIT",
    "PO_BOX_LOBBY": "PO_BOX_LOBBY",
    "PO_BOX_REGION": "PO_BOX_REG",
    "PO_COUNTRY_ISO": "PO_CTRYISO",
    "PO_BOX_NO_NUMBER_FLAG": "PO_W_O_NO",
    "PO_BOX_COUNTRY": "POBOX_CTRY",
    "POSTAL_CODE": "POSTL_COD1",
    "PO_POSTAL_CODE": "POSTL_COD2",
    "COMPANY_POSTAL_CODE": "POSTL_COD3",
    "NAME_PREFIX_1": "PREFIX1",
    "NAME_PREFIX_2": "PREFIX2",
    "PWD_HASH_VALUE": "PWDSALTEDHASH",
    "REFERENCE_USER": "REF_USER",
    "REFERENCE_USERNAME": "REF_USER",
    "REGIONAL_STRUCTURE_GROUPING": "REGIOGROUP",
    "REGION": "REGION",
    "TECH_USER_ACCOUNT_RESPONSIBLE": "RESPONSIBLE",
    "APARTMENT_NUMBER": "ROOM_NO",
    "APARTMENT_NUMBER_P": "ROOM_NO_P",
    "SECOND_NAME": "SECONDNAME",
    "SECURITY_POLICY": "SECURITY_POLICY",
    "SNC": "SNC",
    "SEARCH_TERM_1": "SORT1",
    "SEARCH_TERM_1_P": "SORT1_P",
    "SEARCH_TERM_2": "SORT2",
    "SEARCH_TERM_2_P": "SORT2_P",
    "PRINT_PARAM_3": "SPDA",
    "PRINT_PARAM_2": "SPDB",
    "USER_CLASS_SPECIAL_VERSION": "SPEC_VERS",
    "SPOOL_OUTPUT_DEVICE": "SPLD",
    "PRINT_PARAM_1": "SPLG",
    "START_MENU": "START_MENU",
    "START_MENU_OLD": "STCOD",
    "STREET_ABBREVIATION": "STR_ABBR",
    "STREET_SUPPLEMENT_1": "STR_SUPPL1",
    "STREET_SUPPLEMENT_2": "STR_SUPPL2",
    "STREET_SUPPLEMENT_3": "STR_SUPPL3",
    "STREET": "STREET",
    "STREET_NUMBER": "STREET_NO",
    "USER_CLASSIFICATION_VALID_FROM": "SUBSTITUTE_FROM",
    "USER_CLASSIFICATION_VALID_TO": "SUBSTITUTE_UNTIL",
    "SYSTEM_ID": "SYSID",
    "TAX_JURISDICTION": "TAXJURCODE",
    "TECH_USER_DESCRIPTION": "TECHDESC",
    "TEL_NUMBER_EXTENSION": "TEL1_EXT",
    "TEL_NUMBER": "TEL1_NUMBR",
    "ADDRESS_TIME_ZONE": "TIME_ZONE",
    "TIME_FORMAT": "TIMEFM",
    "TITLE_TEXT": "TITLE",
    "ACADEMIC_TITLE_1": "TITLE_ACA1",
    "ACADEMIC_TITLE_2": "TITLE_ACA2",
    "TITLE_P": "TITLE_P",
    "NAME_SUPPLEMENT": "TITLE_SPPL",
    "TOWNSHIP": "TOWNSHIP",
    "TOWNSHIP_CODE": "TOWNSHIP_CODE",
    "TRANSPORTATION_ZONE": "TRANSPZONE",
    "TIME_ZONE": "TZONE",
    "USER_CLASSIFICATION": "UCLASS",
    "USER_INTERNET_ALIAS": "USERALIAS",
    "USER_NAME": "USERNAME",
    "USER_TYPE": "USTYP",
    "BUSINESS_PURPOSE_FLAG": "XPCPT",
}

# this is a 1:1 mapping because the parameter names are already pretty good
RFC_MAPPING = {
    "ACCEPT_COOKIE": "ACCEPT_COOKIE",
    "ARFC_ACTIVE": "ARFC_ACTIVE",
    "ARFC_CYCLE": "ARFC_CYCLE",
    "ARFC_METHOD": "ARFC_METHOD",
    "ASSERTION_TICKET": "ASSERTION_TICKET",
    "ASSERTION_TICKET_CLIENT": "ASSERTION_TICKET_CLIENT",
    "ASSERTION_TICKET_SYSID": "ASSERTION_TICKET_SYSID",
    "AUTHORIZATION_PARAMETER": "AUTHORIZATION_PARAMETER",
    "BASXML_ACTIVE": "BASXML_ACTIVE",
    "CALLBACK_WHITELIST": "CALLBACK_WHITELIST",
    "CALLBACK_WHITELIST_ACTIVE": "CALLBACK_WHITELIST_ACTIVE",
    "CATEGORY": "CATEGORY",
    "CLIENT_CODEPAGE_ACTIVE": "CLIENT_CODEPAGE_ACTIVE",
    "COMPRESS_REPLY": "COMPRESS_REPLY",
    "CONVERSION_BYTES": "CONVERSION_BYTES",
    "CONVERSION_MODE": "CONVERSION_MODE",
    "CPIC_TIMEOUT": "CPIC_TIMEOUT",
    "DESCRIPTION": "DESCRIPTION",
    "ENABLE_TRACE": "ENABLE_TRACE",
    "EXPLICIT_CODEPAGE": "EXPLICIT_CODEPAGE",
    "EXPLICIT_CODEPAGE_ACTIVE": "EXPLICIT_CODEPAGE_ACTIVE",
    "EXPORT_TRACE": "EXPORT_TRACE",
    "GATEWAY_HOST": "GATEWAY_HOST",
    "GATEWAY_SERVICE": "GATEWAY_SERVICE",
    "GROUP_NAME": "GROUP_NAME",
    "HTTP_COMPRESS": "HTTP_COMPRESS",
    "HTTP_TIMEOUT": "HTTP_TIMEOUT",
    "HTTP_VERSION": "HTTP_VERSION",
    "KEEP_PASSWORD": "KEEP_PASSWORD",
    "KEEP_PROXY_PASSWORD": "KEEP_PROXY_PASSWORD",
    "KEEPALIVE_TIMEOUT": "KEEPALIVE_TIMEOUT",
    "LANGUAGE_CODEPAGE_ACTIVE": "LANGUAGE_CODEPAGE_ACTIVE",
    "LOAD_BALANCING": "LOAD_BALANCING",
    "LOGON_CLIENT": "LOGON_CLIENT",
    "LOGON_LANGUAGE": "LOGON_LANGUAGE",
    "LOGON_METHOD": "LOGON_METHOD",
    "LOGON_USER": "LOGON_USER",
    "LOGON_USER_254": "LOGON_USER_254",
    "MDMP_LIST": "MDMP_LIST",
    "MDMP_SETTINGS_ACTIVE": "MDMP_SETTINGS_ACTIVE",
    "METHOD": "METHOD",
    "NAME": "NAME",
    "PATH_PREFIX": "PATH_PREFIX",
    "PROGRAM": "PROGRAM",
    "PROXY_SERVER": "PROXY_SERVER",
    "PROXY_SERVICE_NUMBER": "PROXY_SERVICE_NUMBER",
    "PROXY_USER": "PROXY_USER",
    "QRFC_VERSION": "QRFC_VERSION",
    "REFERENCE": "REFERENCE",
    "RFC_BITMAP": "RFC_BITMAP",
    "RFC_WAN": "RFC_WAN",
    "RFCLOGON_GUI": "RFCLOGON_GUI",
    "SAME_USER": "SAME_USER",
    "SAVE_AS_HOSTNAME": "SAVE_AS_HOSTNAME",
    "SERVER_NAME": "SERVER_NAME",
    "SERVICE_NUMBER": "SERVICE_NUMBER",
    "SNC_ACTIVE": "SNC_ACTIVE",
    "SNC_PARAMETER": "SNC_PARAMETER",
    "SSL_ACTIVE": "SSL_ACTIVE",
    "SSL_APPLICATION": "SSL_APPLICATION",
    "SSO_TICKET": "SSO_TICKET",
    "START_TYPE": "START_TYPE",
    "SYSTEM_IDENTIFIER": "SYSTEM_IDENTIFIER",
    "SYSTEM_NUMBER": "SYSTEM_NUMBER",
    "TRACE_SETTINGS": "TRACE_SETTINGS",
    "TRFC_BG_DELAY": "TRFC_BG_DELAY",
    "TRFC_BG_REPETITIONS": "TRFC_BG_REPETITIONS",
    "TRFC_BG_SUPRESS": "TRFC_BG_SUPRESS",
    "TRUSTED_SYSTEM": "TRUSTED_SYSTEM",
    "UI_LOCK": "UI_LOCK",
    "UNICODE_BYTES": "UNICODE_BYTES",
    "UPDATE_ALL": "UPDATE_ALL",
    "UPDATE_FIELDS": "UPDATE_FIELDS",
}

# JOB_HEADER_MAPPING = {<HUMAN_READABLE_NAME>: <SAP_KEY>}
JOB_HEADER_MAPPING = {
    "PLANNED_START_DATE": "SDLSTRTDT",  # PLANNED START DATE FOR BACKGROUND JOB
    "PLANNED_START_TIME": "SDLSTRTTM",  # PLANNED START TIME FOR BACKGROUND JOB
    "LAST_START_DATE": "LASTSTRTDT",  # LATEST EXECUTION DATE FOR A BACKGROUND JOB
    "LAST_START_TIME": "LASTSTRTTM",  # LATEST EXECUTION TIME FOR BACKGROUND JOB
    "PREDECESSOR_JOB_NAME": "PREDJOB",  # NAME OF PREVIOUS JOB
    "PREDECESSOR_JOB_ID": "PREDJOBCNT",  # JOB ID
    "JOB_STATUS_CHECK": "CHECKSTAT",  # JOB STATUS CHECK INDICATOR FOR SUBSEQUENT JOB START
    "EVENT_ID": "EVENTID",  # BACKGROUND PROCESSING EVENT
    "EVENT_PARAM": "EVENTPARM",  # BACKGROUND EVENT PARAMETERS (SUCH AS, JOBNAME/JOBCOUNT)
    "DURATION_MIN": "PRDMINS",  # DURATION PERIOD (IN MINUTES) FOR A BATCH JOB
    "DURATION_HOURS": "PRDHOURS",  # DURATION PERIOD (IN HOURS) FOR A BATCH JOB
    "DURATION_DAYS": "PRDDAYS",  # DURATION (IN DAYS) OF DBA ACTION
    "DURATION_WEEKS": "PRDWEEKS",  # DURATION PERIOD (IN WEEKS) FOR A BATCH JOB
    "DURATION_MONTHS": "PRDMONTHS",  # DURATION PERIOD (IN MONTHS) FOR A BATCH JOB
    "PERIODIC": "PERIODIC",  # PERIODIC JOBS INDICATOR
    "CALENDAR_ID": "CALENDARID",  # FACTORY CALENDAR ID FOR BACKGROUND PROCESSING
    "CAN_START_IMMEDIATELY": "IMSTRTPOS",  # FLAG INDICATING WHETHER JOB CAN BE STARTED IMMEDIATELY
    "PERIODIC_BEHAVIOR": "PRDBEHAV",  # PERIOD BEHAVIOR OF JOBS ON NON-WORKDAYS
    "WORKDAY_NUMBER": "WDAYNO",  # NO. OF WORKDAY ON WHICH A JOB IS TO START
    "WORKDAY_COUNT_DIRECTION": "WDAYCDIR",  # COUNT DIRECTION FOR 'ON WORKDAY' START DATE OF A JOB
    "NOT_RUN_BEFORE": "NOTBEFORE",  # PLANNED START DATE FOR BACKGROUND JOB
    "OPERATION_MODE": "OPMODE",  # NAME OF OPERATION MODE
    "LOGICAL_SYSTEM": "LOGSYS",  # LOGICAL SYSTEM
    "OBJECT_TYPE": "OBJTYPE",  # OBJECT TYPE
    "OBJECT_KEY": "OBJKEY",  # OBJECT KEY
    "DESCRIBE_FLAG": "DESCRIBE",  # DESCRIBE FLAG
    "TARGET_SERVER": "TSERVER",  # SERVER NAME
    "TARGET_HOST": "THOST",  # TARGET SYSTEM TO RUN BACKGROUND JOB
    "TARGET_SERVER_GROUP": "TSRVGRP",  # SERVER GROUP NAME BACKGROUND PROCESSING
}

# JOB_STEPS_MAPPING = {<HUMAN_READABLE_NAME>: <SAP_KEY>}
JOB_STEPS_MAPPING = {
    "PROGRAM_NAME": "ABAP_PROGRAM_NAME",  # ABAP PROGRAM NAME
    "PROGRAM_VARIANT": "ABAP_VARIANT_NAME",  # ABAP VARIANT NAME
    "USERNAME": "SAP_USER_NAME",  # SAP USER NAME FOR AUTHORIZATION CHECK
    "LANGUAGE": "LANGUAGE",  # LANGUAGE FOR LIST OUTPUT
}

# job header fields that are mapped to the STARTCOND mask
JOB_MASK_STARTCOND = [
    "WDAYNO",
    "PRDMINS",
    "PRDHOURS",
    "PRDDAYS",
    "PRDWEEKS",
    "PRDMONTHS",
    "SDLSTRTDT",
    "IMSTRTPOS",
    "EVENTID",
    "PREDJOB",
    "OPMODE",
]

# job header fields that are mapped to the RECIPLNT mask
JOB_MASK_RECIPLNT = ["REC_TYPE"]

# job modify step mapping {"<modify param>": "<read param>"}
JOB_STEP_MODIFY_MAPPING = {
    "PROGRAM": "ABAP_PROGRAM_NAME",
    "PARAMETER": "ABAP_VARIANT_NAME",
    "AUTHCKNAM": "SAP_USER_NAME",
    "LANGUAGE": "LANGUAGE",
}


def __virtual__():
    """
    Only load this module if all libraries are available.
    """
    if not PYRFCLIB:
        return False, "Could not load state module, pyrfc unavailable"
    return __virtualname__


def _replace_human_readable(dic, mapping, remove_unknown=True):
    """
    Takes a dictionary and replaces all keys with the SAP names based on a given mapping (recursively).
    Unknown keys will be removed.
    """
    if not isinstance(dic, dict):
        return dic
    else:
        new_d = {}
        for key, value in dic.items():
            new_k = None
            if key.upper() in mapping.keys():
                new_k = mapping[key.upper()]
            elif key.upper() in mapping.values():
                new_k = key.upper()
            elif not remove_unknown:
                log.debug("The key '{}' is not present in the mapping table, but adding anyway")
                new_k = key.upper()
            else:
                continue
            if isinstance(value, dict):
                new_d[new_k] = _replace_human_readable(value, mapping, remove_unknown)
            else:
                new_d[new_k] = value
        return new_d


def _convert_date(datestring):
    """
    Takes a string and converts it into a date.
    """
    log.debug(f"Converting '{datestring}' to date")
    new_d = False
    if isinstance(datestring, date):
        return datestring
    elif isinstance(datestring, int):
        datestring = str(datestring)
    formats = [
        "%Y-%m-%d",  # '2014-12-04'
        "%d-%m-%Y",  # '04-12-2014'
        "%Y%m%d",  # '20141204'
        "%d%m%Y",  # '04122014'
    ]
    for form in formats:
        try:
            new_d = datetime.strptime(datestring, form).date()
        except ValueError:
            continue
        log.debug(f"Used format '{form}' to convert '{datestring}'")
        return new_d
    log.warning(f"Could not convert date '{datestring}' to a python date object")
    return False


def _generate_change_flag_dict(dic):
    """
    Generates a dictionary with flags for each key present.
    """
    if not isinstance(dic, dict):
        return dic
    new_d = {}
    for key, value in dic.items():
        if isinstance(value, dict):
            new_d[key] = _generate_change_flag_dict(value)
        else:
            new_d[key] = "X"
    return new_d


def _clear_empty_dict(dic, remove_none=True):
    """
    Recursively remove empty sub dictionaries.
    """
    if not isinstance(dic, dict):
        return dic
    new_d = {}
    for key, value in dic.items():
        if isinstance(value, dict):
            value = _clear_empty_dict(value, remove_none)
            if value:
                new_d[key] = value
        else:
            if not remove_none:
                if value is not None:
                    new_d[key] = value
            else:
                new_d[key] = value
    return new_d


def _extract_changed_from_dict(changed_dict, dic):
    """
    Will extract all data from ``dic`` if corresponding keys exist in ``changed_dict``.
    """
    new_d = {}
    for key, value in changed_dict.items():
        if key in dic and isinstance(value, dict):
            new_d[key] = _extract_changed_from_dict(value, dic[key])
        elif key in dic:
            new_d[dic] = dic[key]
    return new_d


def _get_bapiret2_messages(ret):
    """
    Retrieve all MESSAGE fields from the result and return them as a list.
    """
    if isinstance(ret, dict):
        ret = [ret]
    return [message.get("MESSAGE", "") for message in ret]


def icm_notified(
    name,
    sid,
    client,
    message_server_host,
    message_server_port,
    logon_group,
    username,
    password,
    invalidate_cache=True,
    reset_ni_buffer=True,
):
    """
    Notify the ICM that a PSE has changed and refresh caches if required

    name:
        Name of the PSE file, e.g. ``SAPSSLS.pse``.

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

    invalidate_cache
        Boolean if the ICM cache should be invalidated, default is ``True``.

    reset_ni_buffer
        Boolean if the network interface buffer should be reset, default is ``True``.

    .. note::
        If the ``SAP_BASIS`` release of the system is <= 701, you need to restart the ICM.

    Example:

    .. code-block:: jinja

        ICM on S4H is notified on changes to SSLS PSEs:
          sap_nwabap.icm_notified:
            - name: SAPSSLS.pse
            - invalidate_cache: True
            - reset_ni_buffer: True
            - sid: S4H
            - client: "000"
            - message_server_host: s4h
            - message_server_port: 3600
            - logon_group: SPACE
            - username: SALT
            - password: __slot__:salt:vault.read_secret(path="nwabap/S4H/000", key="SALT")
    """
    log.debug("Running function")
    ret = {"name": name, "changes": {"old": [], "new": []}, "comment": "", "result": False}
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
        log.debug(f"Notifying ICM of system {sid}")
        if __opts__["test"]:
            ret["changes"]["new"].append("Would have notified ICM about PSE changes")
        else:
            function_modules = {"ICM_SSL_PSE_CHANGED": {"GLOBAL": 1, "CRED_NAME": name}}
            success, _ = __salt__["sap_nwabap.call_fms"](
                function_modules=function_modules, conn=conn
            )
            if not success:
                msg = f"Could not notify the ICM of system {sid}"
                log.error(msg)
                ret["comment"] = msg
                ret["result"] = False
                return ret
            ret["changes"]["new"].append("Notified ICM about PSE changes")

        if invalidate_cache:
            log.debug("Invalidating cache")
            if __opts__["test"]:
                ret["changes"]["new"].append("Would have invalidated ICM cache")
            else:
                function_modules = {
                    "ICM_CACHE_INVALIDATE_ALL": {
                        "GLOBAL": 1,
                    }
                }
                success, _ = __salt__["sap_nwabap.call_fms"](
                    function_modules=function_modules, conn=conn
                )
                if not success:
                    msg = f"Could not invalidate the ICM cache of system {sid}"
                    log.error(msg)
                    ret["comment"] = msg
                    ret["result"] = False
                    return ret
                ret["changes"]["new"].append("Invalidated ICM cache")

        if reset_ni_buffer:
            log.debug("Resetting ICM network interfaces buffers")
            if __opts__["test"]:
                ret["changes"]["new"].append("Would have reset ICM network interface buffer")
            else:
                function_modules = {"ICM_RESET_NIBUFFER": {"GLOBAL": 1}}
                success, _ = __salt__["sap_nwabap.call_fms"](
                    function_modules=function_modules, conn=conn
                )
                if not success:
                    msg = f"Could not reset the ICM network interface buffer of system {sid}"
                    log.error(msg)
                    ret["comment"] = msg
                    ret["result"] = False
                    return ret
                ret["changes"]["new"].append("Reset ICM network interface buffer")
    if __opts__["test"]:
        ret["comment"] = "Would have notified ICM"
    else:
        ret["comment"] = "Notified ICM"
    ret["result"] = True if (not __opts__["test"] or not ret["changes"]) else None
    return ret


def icm_restarted(
    name,
    sid,
    client,
    message_server_host,
    message_server_port,
    logon_group,
    username,
    password,
    restart_mode="soft",
):
    """
    Ensure that the ICM is restarted

    name:
        An arbitrary string.

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

    restart_mode
        Restart mode, either ``soft`` or ``hard``, default is ``soft``.

    Example:

    .. code-block:: jinja

        ICM on S4H is restarted on changes to SSLS PSEs:
          sap_nwabap.icm_restarted:
            - name: ICM restarted
            - sid: S4H
            - client: "000"
            - message_server_host: s4h
            - message_server_port: 3600
            - logon_group: SPACE
            - username: SALT
            - password: __slot__:salt:vault.read_secret(path="nwabap/S4H/000", key="SALT")
            - restart_mode: hard
    """
    log.debug("Running function")
    ret = {"name": name, "changes": {}, "comment": "", "result": False}
    if restart_mode.lower() == "soft":
        restart_mode = 15
    elif restart_mode.lower() == "hard":
        restart_mode = 16
    else:
        msg = f"Unknown restart mode '{restart_mode}'"
        log.error(msg)
        ret["comment"] = msg
        ret["result"] = False
        return ret

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
        log.debug(f"Restarting the ICM of system {sid}")
        if __opts__["test"]:
            ret["comment"] = "Would have restarted the ICM"
            ret["changes"] = {"old": "Running", "new": "Would have been restarted"}
        else:
            function_modules = {"ICM_SHUTDOWN_ICM": {"GLOBAL": 1, "HOW": restart_mode}}
            success, _ = __salt__["sap_nwabap.call_fms"](
                function_modules=function_modules, conn=conn
            )
            if not success:
                msg = f"Could not restart the ICM of system {sid}"
                log.error(msg)
                ret["comment"] = msg
                ret["result"] = False
                return ret
            ret["comment"] = "Restarted the ICM"
            ret["changes"] = {"old": "Running", "new": "Restarted"}
    ret["result"] = True if (not __opts__["test"] or not ret["changes"]) else None
    return ret


# pylint: disable=unused-argument
def user_present(
    name,
    sid,
    client,
    message_server_host,
    message_server_port,
    logon_group,
    username,
    password,
    user_password=None,
    attributes=None,
    roles=None,
    profiles=None,
    unlock_user=True,
    **kwargs,
):
    """
    Ensures that a user is present in the SAP system.

    name
        Username.

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

    user_password
        Password for the user. If None, no one will be set.

    attributes
        All attributes of the user object as a dictionary (see below).

    roles
        List of roles to assign to the user; list of dictionaries:

        .. code-block:: yaml

            - name: <role name>
              valid_from: <date string>  # default is today
              valid_to: <date string>    # default 31-12-9999
            - name: Z_MY_ROLE_1
            - name: Z_MY_ROLE_2
              valid_from: 30-11-2000
              valid_to: 99991231

    profiles
        List of profiles to assign to the user (list of strings).

    unlock_user
        ``True|False`` if the user should be unlocked, default is ``True``.

    The dictionary provided over ``attributes`` must look like the following. Alternativly, the SAP struct
    names (see constant ``USER_MAPPING``) can be used. Both upper- and lowercase are supported.

    .. code-block:: yaml

        address_data:
          address_number: <value>
          address_notes: <value>
          birth_name: <value>
          building_code_long: <value>
          building_code: <value>
          building_code_p: <value>
          co_name: <value>
          city_file_status: <value>
          city: <value>
          city_code: <value>
          communication_method: <value>
          country_key: <value>
          country_key_iso: <value>
          county: <value>
          county_code: <value>
          delivery_service_number: <value>
          delivery_service_type: <value>
          post_delivery_district: <value>
          department: <value>
          district_code: <value>
          district: <value>
          po_box_address: <value>
          street_address: <value>
          email: <value>
          fax_number_extension: <value>
          fax_number: <value>
          first_name: <value>
          building_floor: <value>
          building_floor_p: <value>
          full_name: <value>
          full_name_status: <value>
          function: <value>
          home_city: <value>
          home_city_code: <value>
          house_number: <value>
          house_number_supplement: <value>
          house_numer_range: <value>
          postal_code_internal: <value>
          initials: <value>
          short_name: <value>
          language_key: <value>
          language_record_creation: <value>
          language_key_sap: <value>
          language_key_p: <value>
          language_key_sap_cp: <value>
          language_key_sap_p: <value>
          last_name: <value>
          location: <value>
          middle_name: <value>
          name_country_format_rule: <value>
          name: <value>
          name_2: <value>
          name_3: <value>
          name_4: <value>
          name_format: <value>
          nickname: <value>
          po_box_city_code: <value>
          postal_code_extension_1: <value>
          postal_code_extension_2: <value>
          postal_code_extension_3: <value>
          person_number: <value>
          po_box: <value>
          po_box_city: <value>
          po_box_lobby: <value>
          po_box_region: <value>
          po_country_iso: <value>
          po_box_no_number_flag: <value>
          po_box_country: <value>
          postal_code: <value>
          po_postal_code: <value>
          company_postal_code: <value>
          name_prefix_1: <value>
          name_prefix_2: <value>
          regional_structure_grouping: <value>
          region: <value>
          apartment_number: <value>
          apartment_number_p: <value>
          second_name: <value>
          search_term_1: <value>
          search_term_1_p: <value>
          search_term_2: <value>
          search_term_2_p: <value>
          street_abbreviation: <value>
          street_supplement_1: <value>
          street_supplement_2: <value>
          street_supplement_3: <value>
          street: <value>
          street_number: <value>
          tax_jurisdiction: <value>
          tel_number_extension: <value>
          tel_number: <value>
          address_time_zone: <value>
          title_text: <value>
          academic_title_1: <value>
          academic_title_2: <value>
          title_p: <value>
          name_supplement: <value>
          township: <value>
          township_code: <value>
          transpzone: <value>
          business_purpose_flag: <value>
        username_alias:
          useralias: <value>
        cua_redistribution: <value>
        company:
          company_address: <value>
        user_defaults:
          catt_test_status: <value>
          date_format: <value>
          decimal_format: <value>
          user_defaults: <value>
          cost_center: <value>
          logon_language: <value>
          print_param_3: <value>
          print_param_2: <value>
          spool_output_device: <value>
          print_param_1: <value>
          start_menu: <value>
          start_menu_old: <value>
          time_format: <value>
          description:
            tech_user_account_responsible: <value>
            techdesc: <value>
        external_id_change_indicator:
          external_id: <value>
        logon_data:
          account_id: <value>
          pwd_hash_key: <value>
          user_group: <value>
          pwd_hash_code_version_c: <value>
          pwd_hash_version: <value>
          pwd_hash_code_version_s: <value>
          user_valid_to: <value>
          user_valid_from: <value>
          last_logon_time: <value>
          pwd_hash_value_sha1: <value>
          pwd_hash_value: <value>
          security_policy: <value>
          time_zone: <value>
          user_type: <value>
        reference_user:
          reference_username: <value>
        snc:
          snc_allow_pw_logon: <value>
          snc_printable_name: <value>
        user_classification:
          chargable_user: <value>
          client: <value>
          country_surcharge: <value>
          license_type: <value>
          user_class_special_version: <value>
          substitute_from: <value>
          substitute_until: <value>
          system_id: <value>
          user_classification: <value>

    .. warning::
        This state will not check if the inputs in terms of user data are valid!

    Example:

    .. code-block:: jinja

        Technical user SALT for SAP system S4H / client 000 is present:
          sap_nwabap.user_present:
            - name: SALT
            - sid: S4H
            - client: "000"
            - message_server_host: s4h
            - message_server_port: 3600
            - logon_group: SPACE
            - username: DDIC
            - password: __slot__:salt:vault.read_secret(path="nwabap/S4H/000", key="DDIC")
            - user_password: __slot__:salt:vault.read_secret(path="nwabap/S4H/000", key="SALT")
            - attributes:
                logon_data:
                  user_type: B
                  user_valid_to: "99991231"
                address_data:
                  first_name: SALT_SERVICE_USER
                  last_name: SALT_SERVICE_USER
            - roles:
              - name: Z_SALT_ROLE
                valid_to: 99991231
            - profiles:
              - SAP_ALL
            - unlock_user: True
    """

    def _compare_role_lists(old, new):
        """
        Takes two lists of role dictionaries and compares them.
        """
        dict_representation_old = {
            e["AGR_NAME"]: {"FROM_DAT": e["FROM_DAT"], "TO_DAT": e["TO_DAT"]} for e in old
        }
        dict_representation_new = {
            e["AGR_NAME"]: {"FROM_DAT": e["FROM_DAT"], "TO_DAT": e["TO_DAT"]} for e in new
        }
        # compare elements
        if dict_representation_old.keys() != dict_representation_new.keys():
            return False
        # compare validity dates
        for k, v_old in dict_representation_old.items():
            if v_old.get("FROM_DAT", date.today()) > dict_representation_new[k].get(
                "FROM_DAT", date.today()
            ):
                return False
            if v_old.get("TO_DAT", date(9999, 12, 31)) != dict_representation_new[k].get(
                "TO_DAT", date(9999, 12, 31)
            ):
                return False
        return True

    def _convert_date_fields(dic):
        """
        Convert the date fields from string to dict
        """
        log.debug(f"Converting date fields of {dic}")
        if dic.get("LOGONDATA", {}).get("GLTGB"):
            log.debug("Converting LOGONDATA:GLTGB to date object")
            converted_date = _convert_date(dic["LOGONDATA"]["GLTGB"])
            if not converted_date:
                return False
            dic["LOGONDATA"]["GLTGB"] = converted_date
        if dic.get("LOGONDATA", {}).get("GLTGV"):
            log.debug("Converting LOGONDATA:GLTGB to date object")
            converted_date = _convert_date(dic["LOGONDATA"]["GLTGV"])
            if not converted_date:
                return False
            dic["LOGONDATA"]["GLTGV"] = converted_date
        if dic.get("UCLASS", {}).get("SUBSTITUTE_FROM"):
            log.debug("Converting UCLASS:SUBSTITUTE_FROM to date object")
            converted_date = _convert_date(dic["UCLASS"]["SUBSTITUTE_FROM"])
            if not converted_date:
                return False
            dic["UCLASS"]["SUBSTITUTE_FROM"] = converted_date
        if dic.get("UCLASS", {}).get("SUBSTITUTE_UNTIL"):
            log.debug("Converting UCLASS:SUBSTITUTE_UNTIL to date object")
            converted_date = _convert_date(dic["UCLASS"]["SUBSTITUTE_UNTIL"])
            if not converted_date:
                return False
            dic["UCLASS"]["SUBSTITUTE_UNTIL"] = converted_date
        return dic

    log.debug("Running function")
    ret = {"name": name, "changes": {"old": {}, "new": {}}, "comment": "", "result": False}
    user = name.upper()
    if not roles:
        roles = []
    if not profiles:
        profiles = []
    log.debug(f"Roles to set: {roles}")
    log.debug(f"Profiles to set: {profiles}")

    log.debug("Parsing attributes and replacing human-readable ")
    if not attributes:
        attributes = {}
    else:
        attributes = _replace_human_readable(attributes, mapping=USER_MAPPING, remove_unknown=True)

    log.debug("Converting date field inputs")
    attributes = _convert_date_fields(attributes)
    if isinstance(attributes, bool) and not attributes:
        msg = "Invalid value for a date field"
        log.error(msg)
        ret["comment"] = msg
        ret["result"] = False
        return ret

    log.debug("Get usertype from input")
    user_type = attributes.get("LOGONDATA", {}).get("USTYP", None)

    log.debug("Creating one connection for the lifetime of this state")
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
        log.debug(f"Retrieving information about {user} from SAP system {sid}")
        function_modules = {"BAPI_USER_GET_DETAIL": {"USERNAME": user}}
        success, result = __salt__["sap_nwabap.call_fms"](
            function_modules=function_modules, conn=conn
        )
        if not success:
            msg = f"Could not retrieve information about {user} from SAP system {sid}"
            log.error(msg)
            ret["comment"] = msg
            ret["result"] = False
            return ret

        log.trace(f"Got user:\n{result}")

        user_exists = __salt__["sap_nwabap.process_bapiret2"](
            result["BAPI_USER_GET_DETAIL"]["RETURN"]
        )
        if user_exists:
            log.debug("User already exists, checking for required updates")
            profiles_in_sys = [
                x["BAPIPROF"] for x in result["BAPI_USER_GET_DETAIL"].pop("PROFILES", [])
            ]
            roles_in_sys = result["BAPI_USER_GET_DETAIL"].pop("ACTIVITYGROUPS", [])
            log.debug(f"Existing profiles: {profiles_in_sys}")
            log.debug(f"Existing roles: {roles_in_sys}")

            log.debug("Converting date field inputs")
            user_details = _convert_date_fields(result["BAPI_USER_GET_DETAIL"])
            if isinstance(user_details, bool) and not user_details:
                msg = "Invalid value for a date field"
                log.error(msg)
                ret["comment"] = msg
                ret["result"] = False
                return ret

            # check if we need to unlock as long as we have the results
            lock_status = user_details["ISLOCKED"]
            locked = False
            for lock_type in ["WRNG_LOGON", "LOCAL_LOCK", "GLOB_LOCK"]:
                if lock_status[lock_type] == "L":
                    locked = True
                    break
            if unlock_user and locked:
                log.debug(f"User {user} is locked and needs to be unlocked")
                if not __opts__["test"]:
                    function_modules = {
                        "BAPI_USER_UNLOCK": {"USERNAME": user},
                        "BAPI_TRANSACTION_COMMIT": {"WAIT": "X"},
                    }
                    success, result = __salt__["sap_nwabap.call_fms"](
                        function_modules=function_modules, conn=conn
                    )
                    if not success:
                        msg = f"Could not unlock {user}"
                        log.error(msg)
                        ret["comment"] = msg
                        ret["result"] = False
                        return ret
                    if not __salt__["sap_nwabap.process_bapiret2"](
                        result["BAPI_USER_UNLOCK"]["RETURN"]
                    ):
                        ret["comment"] = _get_bapiret2_messages(
                            result["BAPI_USER_UNLOCK"]["RETURN"]
                        )
                        ret["result"] = False
                        return ret
                    if not __salt__["sap_nwabap.process_bapiret2"](
                        result["BAPI_TRANSACTION_COMMIT"]["RETURN"]
                    ):
                        ret["comment"] = _get_bapiret2_messages(
                            result["BAPI_TRANSACTION_COMMIT"]["RETURN"]
                        )
                        ret["result"] = False
                        return ret

            if not user_type:
                log.debug("Retrieve user type")
                user_type = user_details["LOGONDATA"]["USTYP"]

            log.debug(f"Calculating diffs for update of user {user}")
            diff = salt.utils.dictdiffer.deep_diff(user_details, attributes)
            log.debug("Removing empty dictionaries")
            diff = _clear_empty_dict(
                diff, remove_none=False
            )  # this will also remove empty "new" and "old" dicts
            new = diff.get("new", {})
            # because we're only interested in the changed fields,
            # we remove unchanged stuff from "old"
            old = _extract_changed_from_dict(new, diff.get("old", {}))
            if new:
                log.debug(f"Data for {user} has changed, we need to update. Changes:\n{new}")
                if not __opts__["test"]:
                    function_modules = {
                        "BAPI_USER_CHANGE": {"USERNAME": user},
                        "BAPI_TRANSACTION_COMMIT": {"WAIT": "X"},
                    }
                    log.debug("Adding change flags")
                    for k in [
                        "LOGONDATA",
                        "DEFAULTS",
                        "ADDRESS",
                        "COMPANY",
                        "SNC",
                        "REF_USER",
                        "ALIAS",
                        "UCLASS",
                        "DESCRIPTION",
                    ]:
                        if k in attributes:
                            attributes[k + "X"] = _generate_change_flag_dict(attributes[k])
                    function_modules = salt.utils.dictupdate.merge(
                        function_modules,
                        {"BAPI_USER_CHANGE": attributes},
                        merge_lists=True,
                        strategy="smart",
                    )
                    success, result = __salt__["sap_nwabap.call_fms"](
                        function_modules=function_modules, conn=conn
                    )
                    if not success:
                        msg = f"Could not change {user}"
                        log.error(msg)
                        ret["comment"] = msg
                        ret["result"] = False
                        return ret
                    if not __salt__["sap_nwabap.process_bapiret2"](
                        result["BAPI_USER_CHANGE"]["RETURN"]
                    ):
                        ret["comment"] = _get_bapiret2_messages(
                            result["BAPI_USER_CHANGE"]["RETURN"]
                        )
                        ret["result"] = False
                        return ret
                    if not __salt__["sap_nwabap.process_bapiret2"](
                        result["BAPI_TRANSACTION_COMMIT"]["RETURN"]
                    ):
                        ret["comment"] = _get_bapiret2_messages(
                            result["BAPI_TRANSACTION_COMMIT"]["RETURN"]
                        )
                        ret["result"] = False
                        return ret

                ret["changes"] = {"new": new, "old": old}

            # only process the password if it is given
            if user_password:
                log.debug("Checking if password needs to be updated")
                log.debug("Checking if logon is possible")
                function_modules = {
                    "SUSR_LOGIN_CHECK_RFC": {"BNAME": user, "PASSWORD": user_password}
                }
                password_correct = True
                try:
                    success, result = __salt__["sap_nwabap.call_fms"](
                        function_modules=function_modules, conn=conn, raise_on_error=True
                    )
                except ABAPApplicationError as aae:
                    # unsuccessful logons are output as exceptions
                    """
                    EXCEPTIONS
                        WAIT = 1                    "
                        USER_LOCKED = 2             "               User is locked
                        USER_NOT_ACTIVE = 3         "
                        PASSWORD_EXPIRED = 4        "
                        WRONG_PASSWORD = 5          "
                        NO_CHECK_FOR_THIS_USER = 6  "
                        PASSWORD_ATTEMPTS_LIMITED = 7  "
                    """  # pylint: disable=pointless-string-statement
                    if aae.key == "WRONG_PASSWORD":
                        log.debug("User password is wrong")
                        password_correct = False
                    else:
                        msg = (
                            f"User {user} is in state {aae.key} which "
                            f"cannot be handled by the state"
                        )
                        log.error(msg)
                        ret["comment"] = msg
                        ret["result"] = False
                        return ret
                if not password_correct:
                    log.debug("Password is not correct, setting")
                    log.debug("Setting initial password")
                    if not __opts__["test"]:
                        function_modules = {
                            "BAPI_USER_CHANGE": {
                                "USERNAME": user,
                                "PASSWORD": {"BAPIPWD": user_password},
                                "PASSWORDX": {"BAPIPWD": "X"},
                            },
                            "BAPI_TRANSACTION_COMMIT": {"WAIT": "X"},
                        }
                        success, result = __salt__["sap_nwabap.call_fms"](
                            function_modules=function_modules, conn=conn
                        )
                        if not success:
                            msg = f"Could not set initial password for {user}"
                            log.error(msg)
                            ret["comment"] = msg
                            ret["result"] = False
                            return ret
                        if not __salt__["sap_nwabap.process_bapiret2"](
                            result["BAPI_USER_CHANGE"]["RETURN"]
                        ):
                            ret["comment"] = _get_bapiret2_messages(
                                result["BAPI_USER_CHANGE"]["RETURN"]
                            )
                            ret["result"] = False
                            return ret
                        if not __salt__["sap_nwabap.process_bapiret2"](
                            result["BAPI_TRANSACTION_COMMIT"]["RETURN"]
                        ):
                            ret["comment"] = _get_bapiret2_messages(
                                result["BAPI_TRANSACTION_COMMIT"]["RETURN"]
                            )
                            ret["result"] = False
                            return ret

                    ret["changes"]["new"]["PASSWORD"] = "XXX-REDACTED-XXX"
                    ret["changes"]["old"]["PASSWORD"] = "XXX-REDACTED-XXX"
        else:
            log.debug("User does not exist, creating")
            if not user_password:
                msg = f"User password is required to create {user}"
                log.error(msg)
                ret["comment"] = msg
                ret["result"] = False
                return ret
            if not __opts__["test"]:
                function_modules = {
                    "BAPI_USER_CREATE1": {
                        "USERNAME": user,
                        "PASSWORD": {"BAPIPWD": user_password},
                    },
                    "BAPI_TRANSACTION_COMMIT": {"WAIT": "X"},
                }
                function_modules = salt.utils.dictupdate.merge(
                    function_modules,
                    {"BAPI_USER_CREATE1": attributes},
                    merge_lists=True,
                    strategy="smart",
                )
                success, result = __salt__["sap_nwabap.call_fms"](
                    function_modules=function_modules, conn=conn
                )
                if not success:
                    msg = f"Could not create user {user}"
                    log.error(msg)
                    ret["comment"] = msg
                    ret["result"] = False
                    return ret

                if not __salt__["sap_nwabap.process_bapiret2"](
                    result["BAPI_USER_CREATE1"]["RETURN"]
                ):
                    ret["comment"] = _get_bapiret2_messages(result["BAPI_USER_CREATE1"]["RETURN"])
                    ret["result"] = False
                    return ret
                if not __salt__["sap_nwabap.process_bapiret2"](
                    result["BAPI_TRANSACTION_COMMIT"]["RETURN"]
                ):
                    ret["comment"] = _get_bapiret2_messages(
                        result["BAPI_TRANSACTION_COMMIT"]["RETURN"]
                    )
                    ret["result"] = False
                    return ret

            ret["changes"]["new"] = {"USERNAME": user, **attributes}

            profiles_in_sys = []
            roles_in_sys = []

        log.debug("Checking for role changes")
        # converting roles to correct format for comparison
        for i in range(0, len(roles)):  # pylint: disable=consider-using-enumerate
            role_from_dat = _convert_date(roles[i].get("valid_from", date.today()))
            role_to_dat = _convert_date(roles[i].get("valid_to", date(9999, 12, 31)))
            roles[i] = {
                "AGR_NAME": roles[i]["name"],
                "FROM_DAT": role_from_dat,
                "TO_DAT": role_to_dat,
            }
        log.debug(f"New roles: {roles}")
        # remove keys to NOT compare
        for i in range(0, len(roles_in_sys)):  # pylint: disable=consider-using-enumerate
            keys = list(
                roles_in_sys[i].keys()
            )  # otherwise the dictionary size would change in the next loop
            for k in keys:
                if k not in ["AGR_NAME", "FROM_DAT", "TO_DAT"]:
                    del roles_in_sys[i][k]
                elif k in ["FROM_DAT", "TO_DAT"]:
                    # convert dates for comparison
                    roles_in_sys[i][k] = _convert_date(roles_in_sys[i][k])
        log.debug(f"Exising roles: {roles_in_sys}")
        if not _compare_role_lists(roles_in_sys, roles):
            log.debug("Roles need to be updated")
            if not __opts__["test"]:
                # this FM will remove all roles that are not listed here
                function_modules = {
                    "BAPI_USER_ACTGROUPS_ASSIGN": {"USERNAME": user, "ACTIVITYGROUPS": roles}
                }
                success, result = __salt__["sap_nwabap.call_fms"](
                    function_modules=function_modules, conn=conn
                )
                if not success:
                    msg = f"Could not update roles for {user}"
                    log.error(msg)
                    ret["comment"] = msg
                    ret["result"] = False
                    return ret
                if not __salt__["sap_nwabap.process_bapiret2"](
                    result["BAPI_USER_ACTGROUPS_ASSIGN"]["RETURN"]
                ):
                    ret["comment"] = _get_bapiret2_messages(
                        result["BAPI_USER_ACTGROUPS_ASSIGN"]["RETURN"]
                    )
                    ret["result"] = False
                    return ret

            ret["changes"]["new"]["ROLES"] = roles
            ret["changes"]["old"]["ROLES"] = roles_in_sys

        log.debug("Checking for profile changes")
        # remove duplicates and sort lists for comparison
        profiles = sorted(list(set(profiles)))
        log.debug(f"New profiles: {profiles}")

        # remove duplicates and sort lists for comparison
        profiles_in_sys = sorted(list(set(profiles_in_sys)))
        # remove profiles that come from roles
        for role in roles:
            function_modules = {"PRGN_GET_PROFILES_OF_ROLE_RFC": {"AGR_NAME": role["AGR_NAME"]}}
            success, result = __salt__["sap_nwabap.call_fms"](
                function_modules=function_modules, conn=conn
            )
            if not success:
                msg = f"Could not retrieve profile of role {role['AGR_NAME']}"
                log.error(msg)
                ret["comment"] = msg
                ret["result"] = False
                return ret
            log.debug(f"Role profiles in the system: {result['PRGN_GET_PROFILES_OF_ROLE_RFC']}")
            if result["PRGN_GET_PROFILES_OF_ROLE_RFC"]["PROFILE"]:
                role_profile = result["PRGN_GET_PROFILES_OF_ROLE_RFC"]["PROFILE"][0]["PROFILE"]
                if role_profile in profiles_in_sys:
                    profiles_in_sys.remove(role_profile)

        log.debug(f"Exising profiles: {profiles_in_sys}")
        if profiles != profiles_in_sys:
            log.debug("Profiles need to be updated")
            # this FM will remove all profiles that are not listed here
            if not __opts__["test"]:
                function_modules = {
                    "BAPI_USER_PROFILES_ASSIGN": {"USERNAME": user, "PROFILES": profiles}
                }
                success, result = __salt__["sap_nwabap.call_fms"](
                    function_modules=function_modules, conn=conn
                )
                if not success:
                    msg = f"Could not update roles for {user}"
                    log.error(msg)
                    ret["comment"] = msg
                    ret["result"] = False
                    return ret
                if not __salt__["sap_nwabap.process_bapiret2"](
                    result["BAPI_USER_PROFILES_ASSIGN"]["RETURN"]
                ):
                    ret["comment"] = _get_bapiret2_messages(
                        result["BAPI_USER_PROFILES_ASSIGN"]["RETURN"]
                    )
                    ret["result"] = False
                    return ret

            ret["changes"]["new"]["PROFILES"] = profiles
            ret["changes"]["old"]["PROFILES"] = profiles_in_sys

    if not ret["changes"].get("new", None) and not ret["changes"].get("old", None):
        ret["changes"] = {}
        ret["comment"] = "No changes required"
    elif __opts__["test"]:
        ret["comment"] = f"Would have maintained user {user}"
    else:
        ret["comment"] = f"Maintained user {user}"
    ret["result"] = True if (not __opts__["test"] or not ret["changes"]) else None
    return ret


# pylint: disable=unused-argument
def user_absent(
    name,
    sid,
    client,
    message_server_host,
    message_server_port,
    logon_group,
    username,
    password,
    **kwargs,
):
    """
    Ensure that a user is absent in the system.

    name
        Username.

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

    Example:

    .. code-block:: jinja

        Technical user SALT for SAP system S4H / client 000 is absent:
          sap_nwabap.user_absent:
            - name: SALT
            - sid: S4H
            - client: "000"
            - message_server_host: s4h
            - message_server_port: 3600
            - logon_group: SPACE
            - username: DDIC
            - password: __slot__:salt:vault.read_secret(path="nwabap/S4H/000", key="DDIC")
    """
    log.debug("Running function")
    ret = {"name": name, "changes": {}, "comment": "", "result": False}
    user = name.upper()

    log.debug("Creating one connection for the lifetime of this state")
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
        log.debug(f"Retrieving information about {user} from SAP system {sid}")
        function_modules = {"BAPI_USER_GET_DETAIL": {"USERNAME": user}}
        success, result = __salt__["sap_nwabap.call_fms"](
            function_modules=function_modules, conn=conn
        )
        if not success:
            msg = f"Could not retrieve information about {user} from SAP system {sid}"
            log.error(msg)
            ret["comment"] = msg
            ret["result"] = False
            return ret

        log.trace(f"Got user:\n{result}")

        user_exists = __salt__["sap_nwabap.process_bapiret2"](
            result["BAPI_USER_GET_DETAIL"]["RETURN"]
        )
        if user_exists:
            log.debug("User exists, deleting")
            if not __opts__["test"]:
                function_modules = {
                    "BAPI_USER_DELETE": {"USERNAME": user},
                    "BAPI_TRANSACTION_COMMIT": {"WAIT": "X"},
                }
                success, result = __salt__["sap_nwabap.call_fms"](
                    function_modules=function_modules, conn=conn
                )
                if not success:
                    msg = f"Could not execute BAPI_USER_DELETE for {sid}, please check logs"
                    log.error(msg)
                    ret["comment"] = msg
                    ret["result"] = False
                    return ret

                log.trace(f"Got result:\n{result}")

                if not __salt__["sap_nwabap.process_bapiret2"](
                    result["BAPI_USER_DELETE"]["RETURN"]
                ):
                    ret["comment"] = _get_bapiret2_messages(result["BAPI_USER_DELETE"]["RETURN"])
                    ret["result"] = False
                    return ret
                if not __salt__["sap_nwabap.process_bapiret2"](
                    result["BAPI_TRANSACTION_COMMIT"]["RETURN"]
                ):
                    ret["comment"] = _get_bapiret2_messages(
                        result["BAPI_TRANSACTION_COMMIT"]["RETURN"]
                    )
                    ret["result"] = False
                    return ret
                ret["comment"] = f"User {user} deleted"
                ret["changes"] = {"old": f"User {user} exists", "new": None}
            else:
                ret["comment"] = f"User {user} would be deleted"
                ret["changes"] = {"old": f"User {user} exists", "new": None}
        else:
            ret["comment"] = "No changes required"
            ret["changes"] = {}

    ret["result"] = True if (not __opts__["test"] or not ret["changes"]) else None

    return ret


# pylint: disable=unused-argument
def user_password_productive(
    name,
    sid,
    client,
    message_server_host,
    message_server_port,
    logon_group,
    username,
    password,
    user_password,
    **kwargs,
):
    """
    Ensure that the given password is set as productive for the user

    name
        Username.

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

    user_password
        Password for the user.

    .. note:
        If the user is a DIALOG user, the state will read the password history size and
        set a sufficient number of random passwords to reset the password history. Last,
        the given password will be set as productive password.

    Example:

    .. code-block:: jinja

        Password for user MMUSTERMANN is productive:
          sap_nwabap.user_absent:
            - name: MMUSTERMANN
            - sid: S4H
            - client: "000"
            - message_server_host: s4h
            - message_server_port: 3600
            - logon_group: SPACE
            - username: SALT
            - password: __slot__:salt:vault.read_secret(path="nwabap/S4H/000", key="SALT")
            - user_password: Abcd1234!
    """
    log.debug("Running function")
    ret = {"name": name, "changes": {}, "comment": "", "result": False}
    user = name.upper()

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
        log.debug("Checking if logon is possible")
        function_modules = {"SUSR_LOGIN_CHECK_RFC": {"BNAME": user, "PASSWORD": user_password}}
        password_correct = True
        try:
            success, result = __salt__["sap_nwabap.call_fms"](
                function_modules=function_modules, conn=conn, raise_on_error=True
            )
        except ABAPApplicationError as aae:
            # unsuccessful logons are output as exceptions
            """
            EXCEPTIONS
                WAIT = 1                    "
                USER_LOCKED = 2             "               User is locked
                USER_NOT_ACTIVE = 3         "
                PASSWORD_EXPIRED = 4        "
                WRONG_PASSWORD = 5          "
                NO_CHECK_FOR_THIS_USER = 6  "
                PASSWORD_ATTEMPTS_LIMITED = 7  "
            """  # pylint: disable=pointless-string-statement
            if aae.key in ["WRONG_PASSWORD", "PASSWORD_EXPIRED"]:
                password_correct = False
            else:
                msg = f"User {user} is in state {aae.key} which cannot be handled by the state"
                log.error(msg)
                ret["comment"] = msg
                ret["result"] = False
                return ret
        if password_correct:
            ret["changes"] = {}
            ret["comment"] = "No changes required"
            ret["result"] = True
        else:
            log.debug(f"Password needs to be updated for {user}")

            log.debug(f"Retrieving user type for {user}")
            function_modules = {"BAPI_USER_GET_DETAIL": {"USERNAME": user}}
            success, result = __salt__["sap_nwabap.call_fms"](
                function_modules=function_modules, conn=conn
            )
            if not success:
                msg = f"Could not retrieve user type of {user} from SAP system {sid}"
                log.error(msg)
                ret["comment"] = msg
                ret["result"] = False
                return ret
            if result["LOGONDATA"]["USTYP"] == "A":
                log.debug(
                    f"User {user} is a dialog user, "
                    "setting multiple initial passwords to clear password history"
                )
                log.debug("Retrieving password history size")
                function_modules = {
                    "SBUF_PARAMETER_GET": {"PARAMETER_NAME": "login/password_history_size"}
                }
                success, result = __salt__["sap_nwabap.call_fms"](
                    function_modules=function_modules, conn=conn
                )
                if not success:
                    msg = "Could not retrieve profile parameter login/password_history_size"
                    log.error(msg)
                    ret["comment"] = msg
                    ret["result"] = False
                    return ret
                history_size = int(result["SBUF_PARAMETER_GET"]["PARAMETER_VALUE"])

                log.debug(
                    f"Setting {history_size} temporary generated passwords to reset password history size"
                )
                initial_passwd = None
                for i in range(0, history_size):
                    log.debug(f"Generating initial password #{i}")
                    function_modules = {"SUSR_GENERATE_PASSWORD": {}}
                    success, result = __salt__["sap_nwabap.call_fms"](
                        function_modules=function_modules, conn=conn
                    )
                    if not success:
                        msg = f"Could not generate a random password #{i}"
                        log.error(msg)
                        ret["comment"] = msg
                        ret["result"] = False
                        return ret
                    if "PASSWORD" not in result["SUSR_GENERATE_PASSWORD"]:
                        ret["comment"] = "Execution of SUSR_GENERATE_PASSWORD failed"
                        ret["result"] = False
                        return ret
                    initial_passwd = result["SUSR_GENERATE_PASSWORD"]["PASSWORD"]

                    log.debug(f"Setting initial password #{i}")
                    if not __opts__["test"]:
                        function_modules = {
                            "BAPI_USER_CHANGE": {
                                "USERNAME": user,
                                "PASSWORD": {"BAPIPWD": initial_passwd},
                                "PASSWORDX": {"BAPIPWD": "X"},
                            },
                            "BAPI_TRANSACTION_COMMIT": {"WAIT": "X"},
                        }
                        success, result = __salt__["sap_nwabap.call_fms"](
                            function_modules=function_modules, conn=conn
                        )
                        if not success:
                            msg = f"Could not set initial password #{i} for {user}"
                            log.error(msg)
                            ret["comment"] = msg
                            ret["result"] = False
                            return ret
                        if not __salt__["sap_nwabap.process_bapiret2"](
                            result["BAPI_USER_CHANGE"]["RETURN"]
                        ):
                            ret["comment"] = _get_bapiret2_messages(
                                result["BAPI_USER_CHANGE"]["RETURN"]
                            )
                            ret["result"] = False
                            return ret
                        if not __salt__["sap_nwabap.process_bapiret2"](
                            result["BAPI_TRANSACTION_COMMIT"]["RETURN"]
                        ):
                            ret["comment"] = _get_bapiret2_messages(
                                result["BAPI_TRANSACTION_COMMIT"]["RETURN"]
                            )
                            ret["result"] = False
                            return ret

            log.debug(f"Setting productive password for {user}")
            if not __opts__["test"]:
                function_modules = {
                    "SUSR_USER_CHANGE_PASSWORD_RFC": {
                        "BNAME": user,
                        "PASSWORD": initial_passwd,
                        "NEW_PASSWORD": user_password,
                    }
                }
                success, result = __salt__["sap_nwabap.call_fms"](
                    function_modules=function_modules, conn=conn
                )
                if not success:
                    msg = f"Could set productive password for {user}"
                    log.error(msg)
                    ret["comment"] = msg
                    ret["result"] = False
                    return ret
                if not __salt__["sap_nwabap.process_bapiret2"](
                    result["SUSR_USER_CHANGE_PASSWORD_RFC"]["RETURN"]
                ):
                    ret["comment"] = _get_bapiret2_messages(
                        __salt__["sap_nwabap.process_bapiret2"](
                            result["SUSR_USER_CHANGE_PASSWORD_RFC"]["RETURN"]
                        )
                    )
                    ret["result"] = False
                    return ret
            ret["changes"] = {"new": {"PASSWORD": "XXX-REDACTED-XXX"}}
            if __opts__["test"]:
                ret["comment"] = f"Would have set productive password set for {user}"
            else:
                ret["comment"] = f"Productive password set for {user}"
            ret["result"] = True if (not __opts__["test"] or not ret["changes"]) else None
    return ret


# pylint: disable=unused-argument
def pse_uploaded(
    name,
    sid,
    client,
    message_server_host,
    message_server_port,
    logon_group,
    username,
    password,
    pse_owner,
    pin=None,
    pse_type=None,
    context=None,
    applic=None,
    pse_name=None,
    **kwargs,
):
    """
    Ensures that a PSE is uploaded to the SAP system. Before the upload takes place, the PSE
    on the filesystem and the PSE in STRUST will be compared.

    name
        Filepath of the PSE Filepath

    pin
        PIN of the PSE file, default is ``None``.

    pse_owner
        Owner of the PSE file.

    pse_type
        PSE type, either ``SSLS``, ``SSLC``, ``SSLA`` or ``None``. If ``None`` (default), the
        arguments ``context`` and ``applic`` must be set.

    context
        See function module ``SSFPSE_FILENAME`` for possible values.

    applic
        See function module ``SSFPSE_FILENAME`` for possible values.

    pse_name
        Name for the PSE resulting from ``context`` and ``applic``.

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

    .. warning::
        This function module does not correctly set the PIN for the ASCS instance PSE after upload,
        leaving the system in an inconsistent state. SAP will not fix this issue (function module
        is not released to customer), so DO NOT use this function module if you have PIN-protected
        PSE files. Note that there are currently no remote-enabled function modules to set PSE
        PINs in STRUST.

    Example:

    .. code-block:: jinja

        SAP NetWeaver AS ABAP S4H SSLS PSE is uploaded:
          sap_nwabap.pse_uploaded:
            - name: /usr/sap/S4H/SYS/sec/SAPSSLS.pse
            - pse_owner: s4hadm
            - pse_type: SSLS
            - sid: S4H
            - client: "000"
            - message_server_host: s4h
            - message_server_port: 3600
            - logon_group: SPACE
            - username: SALT
            - password: __slot__:salt:vault.read_secret(path="nwabap/S4H/000", key="SALT")
            - user_password: Abcd1234!
    """
    log.debug("Running function")

    ret = {
        "name": name,
        "changes": {"new": [], "old": []},
        "result": False,
        "comment": "",
    }

    if not pse_type:
        if not context and not applic and not pse_name:
            msg = "Either pse_type or context and applic and pse_name must be specified"
            log.error(f"{msg}")
            ret["comment"] = msg
            ret["result"] = False
            return ret
    elif pse_type == "SSLS":
        context = "SSLS"
        applic = "DFAULT"
        pse_name = "SAPSSLS.pse"
    elif pse_type == "SSLC":
        context = "SSLC"
        applic = "DFAULT"
        pse_name = "SAPSSLC.pse"
    elif pse_type == "SSLA":
        context = "SSLC"
        applic = "ANONYM"
        pse_name = "SAPSSLA.pse"
    else:
        msg = f"Unknown PSE type '{pse_type}'"
        log.error(f"{msg}")
        ret["comment"] = msg
        ret["result"] = False
        return ret

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
        log.debug(f"Checking validity of PSE {pse_name} on the file")
        function_modules = {"SSFPSE_CHECK": {"PSENAME": pse_name}}
        success, result = __salt__["sap_nwabap.call_fms"](
            function_modules=function_modules, conn=conn
        )
        if not success:
            msg = f"Could not check PSE {name}"
            log.error(msg)
            ret["comment"] = msg
            ret["result"] = False
            return ret
        if result["SSFPSE_CHECK"]["CRC"] != 0:
            log.debug(
                f"PSE {pse_name} on the system is either invalid or does not exist and must be replaced"
            )
            certs_are_equal = False
        else:
            log.debug(f"Retrieving information about {name} in SAP system {sid}")
            function_modules = {"SSFP_GET_PSEINFO": {"CONTEXT": context, "APPLIC": applic}}
            success, result = __salt__["sap_nwabap.call_fms"](
                function_modules=function_modules, conn=conn
            )
            cert_data = result.get("SSFP_GET_PSEINFO", {}).get("CERTIFICATE", None)
            if not success or not cert_data:
                msg = f"Could not retrieve information about {name} in SAP system {sid}"
                log.error(msg)
                ret["comment"] = msg
                ret["result"] = False
                return ret
            log.debug(f"Parsing certificate of {name} in SAP system {sid}")
            function_modules = {"SSFP_PARSECERTIFICATE": {"CERTXSTRING": cert_data}}
            success, result = __salt__["sap_nwabap.call_fms"](
                function_modules=function_modules, conn=conn
            )
            if not success:
                msg = f"Could not parse certificate of {name} in SAP system {sid}"
                log.error(msg)
                ret["comment"] = msg
                ret["result"] = False
                return ret
            validity = result["SSFP_PARSECERTIFICATE"]["CERTATTRIBUTES"]["VALIDITY"]
            pse_sap = {
                "alg": result["SSFP_PARSECERTIFICATE"]["ALGID"],
                "subject": result["SSFP_PARSECERTIFICATE"]["CERTATTRIBUTES"]["SUBJECT"],
                "issuer": result["SSFP_PARSECERTIFICATE"]["CERTATTRIBUTES"]["ISSUER"],
                "serial": result["SSFP_PARSECERTIFICATE"]["CERTATTRIBUTES"]["SNUMBER"],
                "valid_from": "".join(validity.split(" ")[:2]),
                "valid_to": "".join(validity.split(" ")[2:]),
                "fingerprint": result["SSFP_PARSECERTIFICATE"]["CERTATTRIBUTES"]["FINGERPR"],
            }

            log.debug("Reading PSE information from file")
            result = __salt__["sap_pse.get_my_name"](
                pse_file=name,
                pse_pwd=pin,
                runas=pse_owner,
            )
            if not isinstance(result, dict):
                msg = f"Cannot read PSE file {name}"
                log.error(f"{msg}")
                ret["result"] = False
                ret["comment"] = msg
                return ret
            pse_file = result["MY Certificate"]

            log.debug("Converting datetime strings for comparison")

            not_after_sap = datetime.strptime(pse_sap["valid_to"], "%Y%m%d%H%M%S")

            pse_not_after = pse_file["Validity not after"].split("(", 1)[0].strip()
            not_after_file = datetime.strptime(pse_not_after, "%a %b %d %H:%M:%S %Y")

            not_before_sap = datetime.strptime(pse_sap["valid_from"], "%Y%m%d%H%M%S")

            pse_not_before = pse_file["Validity not before"].split("(", 1)[0].strip()
            not_before_pse = datetime.strptime(pse_not_before, "%a %b %d %H:%M:%S %Y")

            log.debug("Comparing certificate attributes")
            certs_are_equal = True
            if pse_sap["serial"] != pse_file["Serial Number"].replace(":", ""):
                msg = (
                    f"Serial numbers of File PSE ({pse_file['Serial Number'].replace(':', '')}) "
                    f"and SAP PSE ({pse_sap['serial']}) do not match"
                )
                log.debug(msg)
                certs_are_equal = False
            elif pse_sap["fingerprint"] != pse_file["Certificate fingerprint (MD5)"]:
                msg = (
                    f"MD5 finger prints of File PSE ({pse_file['Certificate fingerprint (MD5)']}) "
                    f"and SAP PSE ({pse_sap['fingerprint']}) do not match"
                )
                log.debug(msg)
                certs_are_equal = False
            elif abs((not_after_sap - not_after_file).total_seconds()) > 0:
                msg = (
                    f"Not after datetimes of File PSE ({not_after_file}) "
                    f"and SAP PSE ({not_after_sap}) do not match"
                )
                log.debug(msg)
                msg = (
                    "Difference between File PSE <> SAP PSE: "
                    f"{abs((not_after_sap - not_after_file).total_seconds())} seconds"
                )
                log.debug(msg)
                certs_are_equal = False
            elif abs((not_before_sap - not_before_pse).total_seconds()) > 0:
                msg = (
                    f"Not before datetimes of File PSE ({not_before_pse}) and "
                    f"SAP PSE ({not_before_sap}) do not match"
                )
                log.debug(msg)
                msg = (
                    f"Difference between File PSE <> SAP PSE: "
                    f"{abs((not_before_sap - not_before_pse).total_seconds())} seconds"
                )
                log.debug(msg)
                certs_are_equal = False

        if certs_are_equal:
            log.debug("No changes required")
            ret["result"] = True
            ret["changes"] = {}
            ret["comment"] = "No changes required"
            return ret

        log.debug("Certificates do not match")

        log.debug(f"Uploading PSE {name} for {context} / {applic}")
        if not __opts__["test"]:
            function_modules = {
                "SSFR_PSE_UPLOAD": {
                    "IS_STRUST_IDENTITY": {"PSE_CONTEXT": context, "PSE_APPLIC": applic},
                    "IV_FILENAME": name,
                    "IV_REPLACE_EXISTING_PSE": "X",
                }
            }
            if pin is not None:
                function_modules["SSFR_PSE_UPLOAD"]["IV_PSEPIN"] = pin
            success, result = __salt__["sap_nwabap.call_fms"](
                function_modules=function_modules, conn=conn
            )
            if not success:
                msg = f"Could not upload PSE {name} for {context} / {applic}"
                log.error(msg)
                ret["comment"] = msg
                ret["result"] = False
                return ret
            if not __salt__["sap_nwabap.process_bapiret2"](
                result["SSFR_PSE_UPLOAD"]["ET_BAPIRET2"]
            ):
                ret["comment"] = _get_bapiret2_messages(result["SSFR_PSE_UPLOAD"]["ET_BAPIRET2"])
                ret["result"] = False
                return ret
            log.debug(f"Uploaded PSE {name} for {context} / {applic} to {sid}")
        ret["changes"]["new"].append(msg)

        if pse_name:
            log.debug(f"Checking validity of PSE {pse_name} on the file")
            if not __opts__["test"]:
                function_modules = {"SSFPSE_CHECK": {"PSENAME": pse_name}}
                success, result = __salt__["sap_nwabap.call_fms"](
                    function_modules=function_modules, conn=conn
                )
                if not success:
                    msg = f"Could not check PSE {name}"
                    log.error(msg)
                    ret["comment"] = msg
                    ret["result"] = False
                    return ret
                if result["SSFPSE_CHECK"]["CRC"] != 0:
                    msg = f"Check for PSE {name} failed, please check the system"
                    log.error(msg)
                    ret["comment"] = msg
                    ret["result"] = False
                    return ret
                log.debug(f"Checked PSE {pse_name} on {sid} successfully")

    if not ret["changes"].get("new", None):
        ret["changes"] = {}
        ret["comment"] = "No changes required"
    elif __opts__["test"]:
        ret["comment"] = f"Would have maintained PSE for {context} / {applic}"
    else:
        ret["comment"] = f"Maintained PSE for {context} / {applic}"
    ret["result"] = True if (not __opts__["test"] or not ret["changes"]) else None
    return ret


def rfc_dest_present(
    name,
    sid,
    client,
    message_server_host,
    message_server_port,
    logon_group,
    username,
    password,
    dest_type=None,
    dest_password=None,
    keep_password=True,
    keep_proxy_password=True,
    **kwargs,
):
    """
    Ensures that an RFC destination is present in the SAP system.

    name
        Name of the RFC destination.

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

    dest_type
        Type of the destination, required for creation. Can be one of: ``H``, ``G``, ``L``, ``3``, ``T``

    dest_password
        Password to set for the connection.

    keep_password
        ``<True|False>`` if the password should be kept on update (if no explicit password is given).

    keep_proxy_password
        ``<True|False>`` if the proxy password should be kept on update (if no explicit password is given).

    Next to these arguments, additional kwargs can be used to set attributes in the RFC destination.
    The following kwargs are recognized and can be used in upper- and lowercase, depending on the RFC
    destination type:

    .. code-block:: yaml

        accept_cookie: <value>
        arfc_active: <value>
        arfc_cycle: <value>
        arfc_method: <value>
        assertion_ticket: <value>
        assertion_ticket_client: <value>
        assertion_ticket_sysid: <value>
        authorization_parameter: <value>
        basxml_active: <value>
        callback_whitelist: <value>
        callback_whitelist_active: <value>
        category: <value>
        client_codepage_active: <value>
        compress_reply: <value>
        conversion_bytes: <value>
        conversion_mode: <value>
        cpic_timeout: <value>
        description: <value>
        enable_trace: <value>
        explicit_codepage: <value>
        explicit_codepage_active: <value>
        export_trace: <value>
        gateway_host: <value>
        gateway_service: <value>
        group_name: <value>
        http_compress: <value>
        http_timeout: <value>
        http_version: <value>
        keep_password: <value>
        keep_proxy_password: <value>
        keepalive_timeout: <value>
        language_codepage_active: <value>
        load_balancing: <value>
        logon_client: <value>
        logon_language: <value>
        logon_method: <value>
        logon_user: <value>
        logon_user_254: logon_user_254,
        mdmp_list: <value>
        mdmp_settings_active: <value>
        method: <value>
        name: <value>
        path_prefix: <value>
        program: <value>
        proxy_server: <value>
        proxy_service_number: <value>
        proxy_user: <value>
        qrfc_version: <value>
        reference: <value>
        rfc_bitmap: <value>
        rfc_wan: <value>
        rfclogon_gui: <value>
        same_user: <value>
        save_as_hostname: <value>
        server_name: <value>
        service_number: <value>
        snc_active: <value>
        snc_parameter: <value>
        ssl_active: <value>
        ssl_application: <value>
        sso_ticket: <value>
        start_type: <value>
        system_identifier: <value>
        system_number: <value>
        trace_settings: <value>
        trfc_bg_delay: <value>
        trfc_bg_repetitions: <value>
        trfc_bg_supress: <value>
        trusted_system: <value>
        ui_lock: <value>
        unicode_bytes: <value>
        update_all: <value>
        update_fields: <value>

    Example:

    .. code-block:: jinja

        SM_SOLCLNT100_BACK is adapted for SAP NetWeaver AS ABAP system S4H:
          sap_nwabap.rfc_dest_present:
            - name: SM_SOLCLNT100_BACK
            - sid: S4H
            - client: "000"
            - message_server_host: s4h
            - message_server_port: 3600
            - logon_group: SPACE
            - username: SALT
            - password: __slot__:salt:vault.read_secret(path="nwabap/S4H/000", key="SALT")
            - dest_type: 3
            - server_name: /H/saprouter.my.domain.de/S/3299/H/sol
    """
    log.debug("Running function")
    name = name.upper()
    ret = {"name": name, "changes": {"old": {}, "new": {}}, "comment": "", "result": False}
    dest_type = str(dest_type)  # if type = 3 -> salt states like to interpret numbers as int
    log.debug("Parsing attributes and replacing human-readable ")
    attributes = _replace_human_readable(kwargs, mapping=RFC_MAPPING, remove_unknown=True)
    # some values need to be strings
    for key in ["SERVICE_NUMBER", "EXPORT_TRACE", "UNICODE_BYTES", "RFC_BITMAP", "BASXML_ACTIVE"]:
        if key in attributes:
            attributes[key] = str(attributes[key])

    log.debug("Creating one connection for the lifetime of this state")
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

        log.debug(f"Checking if RFC destination {name} exists")
        function_modules = {"DEST_EXISTS": {"NAME": name}}
        success, result = __salt__["sap_nwabap.call_fms"](
            function_modules=function_modules, conn=conn
        )
        if not success or "EXISTS" not in result["DEST_EXISTS"]:
            msg = f"Could not check existance of {name}"
            log.error(msg)
            ret["comment"] = msg
            ret["changes"] = {}
            ret["result"] = False
            return ret
        update_password = False
        if result["DEST_EXISTS"]["EXISTS"] == "X":
            log.debug(f"RFC destination {name} already exists")
            log.debug(f"Determining type for {name}")
            function_modules = {"DEST_GET_TYPE": {"NAME": name}}
            success, result = __salt__["sap_nwabap.call_fms"](
                function_modules=function_modules, conn=conn
            )
            if not success or "DEST_TYPE" not in result["DEST_GET_TYPE"]:
                msg = f"Could not determine type of {name}"
                log.error(msg)
                ret["comment"] = msg
                ret["changes"] = {}
                ret["result"] = False
                return ret
            dest_type_existing = result["DEST_GET_TYPE"]["DEST_TYPE"]["RFCTYPE"]
            if not dest_type:
                dest_type = dest_type_existing
                log.debug(f"RFC destination {name} is of type {dest_type}")
            elif dest_type != dest_type_existing:
                msg = (
                    f"RFC destination {name} already exists as type '{dest_type_existing}' with "
                    f"type '{dest_type}' being requested. Switching types is not supported"
                )
                log.error(msg)
                ret["comment"] = msg
                ret["changes"] = {}
                ret["result"] = False
                return ret
            log.debug(f"Reading existing data for destination {name}")
            if dest_type == "H":
                read_fm = "DEST_HTTP_ABAP_READ"
            elif dest_type == "G":
                read_fm = "DEST_HTTP_EXT_READ"
            elif dest_type == "L":
                read_fm = "DEST_LOGICAL_READ"
            elif dest_type == "3":
                read_fm = "DEST_RFC_ABAP_READ"
            elif dest_type == "T":
                read_fm = "DEST_RFC_TCPIP_READ"
            else:
                msg = f"RFC destination {name} is of the unsupported type {dest_type}"
                log.error(msg)
                ret["changes"] = {}
                ret["comment"] = msg
                ret["result"] = False
                return ret
            success, result = __salt__["sap_nwabap.call_fms"](
                function_modules={read_fm: {"NAME": name}}, conn=conn
            )
            if not success:
                msg = f"Could not read {name}"
                log.error(msg)
                ret["changes"] = {}
                ret["comment"] = msg
                ret["result"] = False
                return ret
            data = result[read_fm]

            log.debug(f"Calculating diffs for update of RFC destination {name}")
            diff = salt.utils.dictdiffer.deep_diff(data, attributes)
            log.debug("Removing empty dictionaries")
            diff = _clear_empty_dict(
                diff, remove_none=False
            )  # this will also remove empty "new" and "old" dicts
            new = diff.get("new", {})
            # because we're only interested in the changed fields, we remove unchanged stuff from "old"
            old = _extract_changed_from_dict(new, diff.get("old", {}))
            if new:
                log.debug(f"Data for {name} has changed, we need to update. Changes:\n{new}")
                log.debug("Generating list of fields to update")
                update_data = new
                update_data["NAME"] = name
                update_data["UPDATE_ALL"] = " "  # required since default value is "X"
                update_data["UPDATE_FIELDS"] = {}
                for field in new:
                    if field not in ["NAME", "UPDATE_FIELDS", "UPDATE_ALL"]:
                        update_data["UPDATE_FIELDS"][field] = "X"
                if "LOGON_USER" not in new and keep_password:
                    update_data["KEEP_PASSWORD"] = "X"
                else:
                    update_password = True
                if "PROXY_USER" not in new and dest_type in ["H", "G"] and keep_proxy_password:
                    # only supported for HTTP connections
                    update_data["KEEP_PROXY_PASSWORD"] = "X"

                if dest_type == "H":
                    update_fm = "DEST_HTTP_ABAP_UPDATE"
                elif dest_type == "G":
                    update_fm = "DEST_HTTP_EXT_UPDATE"
                elif dest_type == "L":
                    update_fm = "DEST_LOGICAL_UPDATE"
                elif dest_type == "3":
                    update_fm = "DEST_RFC_ABAP_UPDATE"
                elif dest_type == "T":
                    update_fm = "DEST_RFC_TCPIP_UPDATE"
                else:
                    msg = f"RFC destination {name} is of the unsupported type {dest_type}"
                    log.error(msg)
                    ret["comment"] = msg
                    ret["result"] = False
                    return ret

                if not __opts__["test"]:
                    success, result = __salt__["sap_nwabap.call_fms"](
                        function_modules={update_fm: update_data}, conn=conn
                    )
                    if not success:
                        msg = f"Could not update {name}"
                        log.error(msg)
                        ret["comment"] = msg
                        ret["result"] = False
                        return ret
                ret["changes"] = {"new": new, "old": old}
        else:
            update_password = True
            if not dest_type:
                msg = "RFC destination type 'dest_type' was not provided as argument but is required for creation"
                log.error(msg)
                ret["comment"] = msg
                ret["result"] = False
                return ret
            if dest_type == "H":
                create_fm = "DEST_HTTP_ABAP_CREATE"
            elif dest_type == "G":
                create_fm = "DEST_HTTP_EXT_CREATE"
            elif dest_type == "L":
                create_fm = "DEST_LOGICAL_CREATE"
            elif dest_type == "3":
                create_fm = "DEST_RFC_ABAP_CREATE"
            elif dest_type == "T":
                create_fm = "DEST_RFC_TCPIP_CREATE"
            else:
                msg = f"RFC destination {name} is of the unsupported type {dest_type}"
                log.error(msg)
                ret["comment"] = msg
                ret["result"] = False
                return ret
            attributes["NAME"] = name
            if not __opts__["test"]:
                success, result = __salt__["sap_nwabap.call_fms"](
                    function_modules={create_fm: attributes}, conn=conn
                )
                if not success:
                    msg = f"Could not create RFC destination {name}"
                    log.error(msg)
                    ret["comment"] = msg
                    ret["result"] = False
                    return ret
            ret["changes"]["new"] = {**attributes}

        if dest_password and update_password:
            log.debug(f"Setting password for RFC destination {name}")
            if not __opts__["test"]:
                function_modules = {"DEST_SET_PASSWORD": {"NAME": name, "PASSWORD": dest_password}}
                success, result = __salt__["sap_nwabap.call_fms"](
                    function_modules=function_modules, conn=conn
                )
                if not success:
                    msg = f"Could not set password for RFC destination {name}"
                    log.error(msg)
                    ret["comment"] = msg
                    ret["result"] = False
                    return ret
            ret["changes"]["new"]["PASSWORD"] = "XXX-REDACTED-XXX"

    if not ret["changes"].get("new", None) and not ret["changes"].get("old", None):
        ret["changes"] = {}
        ret["comment"] = "No changes required"
    elif __opts__["test"]:
        ret["comment"] = f"Would have maintained RFC destination {name}"
    else:
        ret["comment"] = f"Maintained RFC destination {name}"
    ret["result"] = True if (not __opts__["test"] or not ret["changes"]) else None
    return ret


# pylint: disable=unused-argument
def rfc_dest_absent(
    name,
    sid,
    client,
    message_server_host,
    message_server_port,
    logon_group,
    username,
    password,
    **kwargs,
):
    """
    Ensures that an RFC destination is absent in the SAP system.

    name
        Name of the RFC destination.

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

    Example:

    .. code-block:: jinja

        SM_SOLCLNT100_BACK is absent from SAP NetWeaver AS ABAP system S4H:
          sap_nwabap.rfc_dest_absent:
            - name: SM_SOLCLNT100_BACK
            - sid: S4H
            - client: "000"
            - message_server_host: s4h
            - message_server_port: 3600
            - logon_group: SPACE
            - username: SALT
            - password: __slot__:salt:vault.read_secret(path="nwabap/S4H/000", key="SALT")
    """
    log.debug("Running function")
    name = name.upper()
    ret = {"name": name, "changes": {"old": {}, "new": {}}, "comment": "", "result": False}
    log.debug("Creating one connection for the lifetime of this state")
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
        log.debug(f"Checking if RFC destination {name} exists")
        function_modules = {"DEST_EXISTS": {"NAME": name}}
        success, result = __salt__["sap_nwabap.call_fms"](
            function_modules=function_modules, conn=conn
        )
        if not success or "EXISTS" not in result["DEST_EXISTS"]:
            msg = f"Could not check existance of {name}"
            log.error(msg)
            ret["comment"] = msg
            ret["result"] = False
            return ret
        if result["DEST_EXISTS"]["EXISTS"] == "X":
            log.debug(f"RFC destination {name} exists, removing")
            if not __opts__["test"]:
                function_modules = {"DEST_DELETE": {"NAME": name}}
                success, result = __salt__["sap_nwabap.call_fms"](
                    function_modules=function_modules, conn=conn
                )
                if not success:
                    msg = f"Could not delete {name}"
                    log.error(msg)
                    ret["comment"] = msg
                    ret["result"] = False
                    return ret
            ret["changes"] = {"old": name, "new": None}
    if not ret["changes"].get("new", None) and not ret["changes"].get("old", None):
        ret["changes"] = {}
        ret["comment"] = "No changes required"
    elif __opts__["test"]:
        ret["comment"] = f"Would have removed RFC destination {name}"
    else:
        ret["comment"] = f"Removed RFC destination {name}"
    ret["result"] = True if (not __opts__["test"] or not ret["changes"]) else None
    return ret


# pylint: disable=unused-argument
def sld_config_present(
    name,
    sid,
    client,
    message_server_host,
    message_server_port,
    logon_group,
    username,
    password,
    **kwargs,
):
    """
    Ensure that an SLD configuration is present in the system.

    name
        Name of the SLD RFC destination.

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

    Example:

    .. code-block:: jinja

        SLD config is present on S4H:
          sap_nwabap.sld_config_present:
            - name: SLD_DS_TARGET
            - sid: S4H
            - client: "000"
            - message_server_host: s4h
            - message_server_port: 3600
            - logon_group: SPACE
            - username: SALT
            - password: __slot__:salt:vault.read_secret(path="nwabap/S4H/000", key="SALT")
    """
    log.debug("Running function")
    name = name.upper()
    ret = {"name": name, "changes": {"old": {}, "new": {}}, "comment": "", "result": False}
    log.debug("Creating one connection for the lifetime of this state")
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
        log.debug("Retrieving current configuration")
        current_config = __salt__["sap_nwabap.read_table"](
            table_name="SLDAGADM",
            fields=["PROGNAME", "ACTIVE", "SEQNR", "RFCDEST", "DORFC", "DOBTC", "BTCMIN"],
            conn=conn,
        )
        update_http_dest = True
        update_bg_job = True
        for line in current_config:
            if line["PROGNAME"] == "HTTP_SLD_DS_TARGET":
                if line["ACTIVE"] == "X" and line["SEQNR"] == "0000" and line["RFCDEST"] == name:
                    update_http_dest = False
                else:
                    ret["changes"]["old"]["HTTP_SLD_DS_TARGET"] = {
                        "ACTIVE": line["ACTIVE"],
                        "SEQNR": line["SEQNR"],
                        "RFCDEST": line["RFCDEST"],
                    }
            elif line["PROGNAME"] == "RSLDAGDS":
                if (
                    line["ACTIVE"] == ""
                    and line["SEQNR"] == "0000"
                    and line["DORFC"] == "X"
                    and line["DOBTC"] == "X"
                    and line["BTCMIN"] == 720
                ):
                    update_bg_job = False
                else:
                    ret["changes"]["old"]["RSLDAGDS"] = {
                        "ACTIVE": line["ACTIVE"],
                        "SEQNR": line["SEQNR"],
                        "DORFC": line["DORFC"],
                        "DOBTC": line["DOBTC"],
                        "BTCMIN": line["BTCMIN"],
                    }

        payload = []
        if update_http_dest:
            log.debug("Setting HTTP_SLD_DS_TARGET")
            payload.append(
                {"PROGNAME": "HTTP_SLD_DS_TARGET", "ACTIVE": "X", "SEQNR": "0000", "RFCDEST": name}
            )
            ret["changes"]["new"]["HTTP_SLD_DS_TARGET"] = {
                "ACTIVE": "X",
                "SEQNR": "0000",
                "RFCDEST": name,
            }
        if update_bg_job:
            log.debug("Setting RSLDAGDS")
            payload.append(
                {
                    "PROGNAME": "RSLDAGDS",
                    "ACTIVE": "",
                    "SEQNR": "0000",
                    "DORFC": "X",
                    "DOBTC": "X",
                    "BTCMIN": 720,
                }
            )
            ret["changes"]["new"]["RSLDAGDS"] = {
                "ACTIVE": "",
                "SEQNR": "0000",
                "DORFC": "X",
                "DOBTC": "X",
                "BTCMIN": 720,
            }
        if payload:
            log.debug("Updating SLD configuration")
            if not __opts__["test"]:
                function_modules = {"SLDAG_SET_CONFIG": {"SLDCFG": payload}}
                success, _ = __salt__["sap_nwabap.call_fms"](
                    function_modules=function_modules, conn=conn
                )
                if not success:
                    msg = "Could not update SLD configuration"
                    log.error(msg)
                    ret["changes"]["new"] = {}
                    ret["comment"] = msg
                    ret["result"] = False
                    return ret

    if not ret["changes"].get("new", None) and not ret["changes"].get("old", None):
        ret["changes"] = {}
        ret["comment"] = "No changes required"
    elif __opts__["test"]:
        ret["comment"] = "Would have updated SLD registration"
    else:
        ret["comment"] = "Updated SLD registration"
    ret["result"] = True if (not __opts__["test"] or not ret["changes"]) else None
    return ret


# pylint: disable=unused-argument
def sld_data_transfered(
    name,
    sid,
    client,
    message_server_host,
    message_server_port,
    logon_group,
    username,
    password,
    **kwargs,
):
    """
    Runs the report RSLDAGDS that triggers the SLD data transfer.

    name
        Name of the SLD RFC destination.

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

    .. note::
        This state will **always** produce changes.

    Example:

    .. code-block:: jinja

        SLD data is transfered for S4H:
          sap_nwabap.sld_data_transfered:
            - name: SLD_DS_TARGET
            - sid: S4H
            - client: "000"
            - message_server_host: s4h
            - message_server_port: 3600
            - logon_group: SPACE
            - username: SALT
            - password: __slot__:salt:vault.read_secret(path="nwabap/S4H/000", key="SALT")
    """
    log.debug("Running function")
    name = name.upper()
    ret = {"name": name, "changes": {"old": {}, "new": {}}, "comment": "", "result": False}
    log.debug("Creating one connection for the lifetime of this state")
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
        log.debug("Executing INST_EXECUTE_REPORT")
        if not __opts__["test"]:
            success, result = __salt__["sap_nwabap.call_fms"](
                function_modules={"INST_EXECUTE_REPORT": {"PROGRAM": "RSLDAGDS"}}, conn=conn
            )
            if not success:
                msg = "Could not execute INST_EXECUTE_REPORT with program RSLDAGDS"
                log.error(msg)
                ret["comment"] = msg
                ret["result"] = False
                return ret

            success_messages = 0
            for line in result["INST_EXECUTE_REPORT"]["OUTPUT_TAB"]:
                # Note: INST_EXECUTE_REPORT only outputs a maximum of 85 chars per line,
                # therefore we need to check for the cut off content
                if "Used HTTP destination: SLD_DS_TAR" in line["ZEILE"]:
                    success_messages += 1
                elif "Data sent with destination SLD_DS" in line["ZEILE"]:
                    success_messages += 1
            if success_messages < 2:
                msg = (
                    "Error during execution of RSLDAGDS: "
                    f"{result['INST_EXECUTE_REPORT']['OUTPUT_TAB']}"
                )
                log.error(msg)
                ret["comment"] = "SLD data was not sent successfully"
                ret["result"] = False
            else:
                ret["comment"] = "SLD data was sent successfully"
                ret["result"] = True
                ret["changes"]["new"] = f"SLD data sent to {name}"
        else:
            ret["comment"] = "SLD data would have been sent successfully"
            ret["result"] = True
            ret["changes"]["new"] = f"SLD data would have been sent to {name}"
    return ret


# pylint: disable=unused-argument
def job_present(
    name,
    jobclass,
    header,
    steps,
    sid,
    client,
    message_server_host,
    message_server_port,
    logon_group,
    username,
    password,
    **kwargs,
):
    """
    Ensure that a job is present in the SAP system.

    name
        Name of the job.

    jobclass
        Class of the job, on of: ``A``, ``B``, ``C``

    header
        Header of the job.

    steps
        List of job steps.

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

    The dictionary provided over ``header`` must look like the SAP struct names or human readable names
    for job headers (see constant ``JOB_HEADER_MAPPING``):

    .. list-table:: JOB_HEADER_MAPPING
       :widths: 25 25 50
       :header-rows: 1

       * - Attribute Name
         - SAP Field
         - Description
       * - planned_start_date
         - SDLSTRTDT
         - Planned start date for background job
       * - planned_start_time
         - SDLSTRTTM
         - Planned start time for background job
       * - last_start_date
         - LASTSTRTDT
         - Latest execution date for a background job
       * - last_start_time
         - LASTSTRTTM
         - Latest execution time for background job
       * - predecessor_job_name
         - PREDJOB
         - Name of previous job
       * - predecessor_job_id
         - PREDJOBCNT
         - Job ID
       * - job_status_check
         - CHECKSTAT
         - Job status check indicator for subsequent job start
       * - event_id
         - EVENTID
         - Background processing event
       * - event_param
         - EVENTPARM
         - Background event parameters (such as jobname/jobcount)
       * - duration_min
         - PRDMINS
         - Duration period (in minutes) for a batch job
       * - duration_hours
         - PRDHOURS
         - Duration period (in hours) for a batch job
       * - duration_days
         - PRDDAYS
         - Duration (in days) of dba action
       * - duration_weeks
         - PRDWEEKS
         - Duration period (in weeks) for a batch job
       * - duration_months
         - PRDMONTHS
         - Duration period (in months) for a batch job
       * - periodic
         - PERIODIC
         - Periodic jobs indicator
       * - calendar_id
         - CALENDARID
         - Factory calendar id for background processing
       * - can_start_immediately
         - IMSTRTPOS
         - Flag indicating whether job can be started immediately
       * - periodic_behavior
         - PRDBEHAV
         - Period behavior of jobs on non-workdays
       * - workday_number
         - WDAYNO
         - No. of workday on which a job is to start
       * - workday_count_direction
         - WDAYCDIR
         - Count direction for 'on workday' start date of a job
       * - not_run_before
         - NOTBEFORE
         - Planned start date for background job
       * - operation_mode
         - OPMODE
         - Name of operation mode
       * - logical_system
         - LOGSYS
         - Logical system
       * - object_type
         - OBJTYPE
         - Object type
       * - object_key
         - OBJKEY
         - Object key
       * - describe_flag
         - DESCRIBE
         - Describe flag
       * - target_server
         - TSERVER
         - Server name
       * - target_host
         - THOST
         - Target system to run background job
       * - target_server_group
         - TSRVGRP
         - Server group name background processing

    The element of the list provided over ``steps`` must look like the SAP struct names or human readable names
    for job steps (see constant ``JOB_STEPS_MAPPING``):

    .. list-table:: JOB_STEPS_MAPPING
       :widths: 25 25 50
       :header-rows: 1

       * - Attribute Name
         - SAP Field
         - Description
       * - program_name
         - ABAP_PROGRAM_NAME
         - ABAP program name
       * - program_variant
         - ABAP_VARIANT_NAME
         - ABAP variant name
       * - username
         - SAP_USER_NAME
         - SAP user name for authorization check
       * - language
         - LANGUAGE
         - Language for list output

    .. note::
        This currently only supports ABAP job steps.

    Example:

    .. code-block:: jinja

        SLD job SAP_SLD_DATA_COLLECT is present on S4H:
          sap_nwabap.job_present:
            - name: SAP_SLD_DATA_COLLECT
            - jobclass: C
            - header:
                EVENTID: SAP_SYSTEM_START
            - steps:
              - ABAP_PROGRAM_NAME: RSLDAGDS
            - sid: S4H
            - client: "000"
            - message_server_host: s4h
            - message_server_port: 3600
            - logon_group: SPACE
            - username: SALT
            - password: __slot__:salt:vault.read_secret(path="nwabap/S4H/000", key="SALT")
    """
    log.debug("Running function")
    name = name.upper()
    jobclass = jobclass.upper()
    ret = {"name": name, "changes": {"old": {}, "new": {}}, "comment": "", "result": False}

    log.debug("Parsing attributes and replacing human-readable")
    header = _replace_human_readable(header, mapping=JOB_HEADER_MAPPING, remove_unknown=True)
    steps = [
        _replace_human_readable(x, mapping=JOB_STEPS_MAPPING, remove_unknown=True) for x in steps
    ]

    log.debug("Creating one connection for the lifetime of this state")
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
        log.debug("Logging in and retrieving session id")
        function_modules = {
            "BAPI_XMI_LOGON": {
                "EXTCOMPANY": "SAPUCC",
                "EXTPRODUCT": "PYTHON",
                "INTERFACE": "XBP",
                "VERSION": "2.0",
            }
        }
        success, result = __salt__["sap_nwabap.call_fms"](
            function_modules=function_modules, conn=conn
        )
        if not success:
            msg = "Could not logon"
            log.error(msg)
            ret["comment"] = msg
            ret["result"] = False
            return ret
        if not __salt__["sap_nwabap.process_bapiret2"](result["BAPI_XMI_LOGON"]["RETURN"]):
            ret["comment"] = _get_bapiret2_messages(result["BAPI_XMI_LOGON"]["RETURN"])
            ret["result"] = False
            return ret
        session_id = result["BAPI_XMI_LOGON"]["SESSIONID"]

        log.debug("Opening a try-catch block to ensure logoff")
        try:
            log.debug(f"Checking if job {name} already exists")
            function_modules = {
                "BAPI_XBP_JOB_SELECT": {
                    "JOB_SELECT_PARAM": {
                        "JOBNAME": name,
                        "USERNAME": "*",  # required for BAPI_XBP_JOB_SELECT to work
                        "PRELIM": "X",
                        "SCHEDUL": "X",
                    },
                    "EXTERNAL_USER_NAME": session_id,
                }
            }
            success, result = __salt__["sap_nwabap.call_fms"](
                function_modules=function_modules, conn=conn
            )
            if not success:
                msg = f"Could not lookup job {name}"
                log.error(msg)
                ret["comment"] = msg
                ret["result"] = False
                return ret
            if not __salt__["sap_nwabap.process_bapiret2"](result["BAPI_XBP_JOB_SELECT"]["RETURN"]):
                ret["comment"] = _get_bapiret2_messages(result["BAPI_XBP_JOB_SELECT"]["RETURN"])
                ret["result"] = False
                return ret

            if result["BAPI_XBP_JOB_SELECT"]["SELECTED_JOBS"]:
                job = result["BAPI_XBP_JOB_SELECT"]["SELECTED_JOBS"][0]
                log.debug(
                    f"Job {job['JOBNAME']} already exists with the id {job['JOBCOUNT']}, retrieving data"
                )
                function_modules = {
                    "BAPI_XBP_JOB_READ": {
                        "JOBNAME": job["JOBNAME"],
                        "JOBCOUNT": job["JOBCOUNT"],
                        "EXTERNAL_USER_NAME": session_id,
                    }
                }
                success, result = __salt__["sap_nwabap.call_fms"](
                    function_modules=function_modules, conn=conn
                )
                if not success:
                    msg = f"Could not read job {name}"
                    log.error(msg)
                    ret["comment"] = msg
                    ret["result"] = False
                    return ret
                if not __salt__["sap_nwabap.process_bapiret2"](
                    result["BAPI_XBP_JOB_READ"]["RETURN"]
                ):
                    ret["comment"] = _get_bapiret2_messages(result["BAPI_XBP_JOB_READ"]["RETURN"])
                    ret["result"] = False
                    return ret
                job_name = job["JOBNAME"]
                job_count = job["JOBCOUNT"]
                job_header = result["BAPI_XBP_JOB_READ"]["JOBHEAD"]
                job_steps = result["BAPI_XBP_JOB_READ"]["STEPS"]

                log.debug(f"Calculating diffs for update of header of {name}")
                diff = salt.utils.dictdiffer.deep_diff(job_header, header)
                log.debug("Removing empty dictionaries")
                diff = _clear_empty_dict(
                    diff, remove_none=False
                )  # this will also remove empty "new" and "old" dicts
                new = diff.get("new", {})
                # because we're only interested in the changed fields, we remove unchanged stuff from "old"
                old = _extract_changed_from_dict(new, diff.get("old", {}))
                if new:
                    log.debug(f"Changing the following fields for {name}:\n{new}")
                    mask = {}
                    # because SAP doesn't like documentation, you have to check the code of FM BP_JOB_HEADER_MODIFY
                    # to find out which MASK flags correspond to which header fields.
                    # in case any of these fields are set, the corresponding mask flag must be set
                    for cond in JOB_MASK_STARTCOND:
                        if new.get(cond, None):
                            mask["STARTCOND"] = "X"
                            break
                    for cond in JOB_MASK_RECIPLNT:
                        if new.get(cond, None):
                            mask["RECIPLNT"] = "X"
                            break
                    # all other fields are 1:1 mappings
                    for field in ["THOST", "TSERVER", "TSRVGRP"]:
                        if new.get(field, None):
                            mask[field] = "X"
                    if not __opts__["test"]:
                        function_modules = {
                            "BAPI_XBP_JOB_HEADER_MODIFY": {
                                "JOBNAME": job_name,
                                "JOBCOUNT": job_count,
                                "EXTERNAL_USER_NAME": session_id,
                                "JOB_HEADER": new,  # we only need the fields the have actually changed
                                "MASK": mask,
                            }
                        }
                        # the job class must be handled separately (thx SAP!)
                        if job_header["JOBCLASS"] != jobclass:
                            function_modules["BAPI_XBP_JOB_HEADER_MODIFY"]["JOBCLASS"] = jobclass
                        success, result = __salt__["sap_nwabap.call_fms"](
                            function_modules=function_modules, conn=conn
                        )
                        if not success:
                            msg = f"Could not modify header for {name}"
                            log.error(msg)
                            ret["comment"] = msg
                            ret["result"] = False
                            return ret
                        if not __salt__["sap_nwabap.process_bapiret2"](
                            result["BAPI_XBP_JOB_HEADER_MODIFY"]["RETURN"]
                        ):
                            ret["comment"] = _get_bapiret2_messages(
                                result["BAPI_XBP_JOB_HEADER_MODIFY"]["RETURN"]
                            )
                            ret["result"] = False
                            return ret
                    # update changes
                    ret["changes"] = {"old": old, "new": new}
                    if job_header["JOBCLASS"] != jobclass:
                        ret["changes"]["old"]["JOBCLASS"] = job_header["JOBCLASS"]
                        ret["changes"]["new"]["JOBCLASS"] = jobclass
                log.debug(f"Checking for changes in the steps of job {name}")
                for i in range(0, len(steps)):  # pylint: disable=consider-using-enumerate
                    if len(job_steps) <= i:
                        log.debug(f"The job step #{i} is missing, adding to {name}")
                        steps[i]["JOBNAME"] = name
                        steps[i]["JOBCOUNT"] = job_count
                        steps[i]["EXTERNAL_USER_NAME"] = session_id
                        if not __opts__["test"]:
                            function_modules = {"BAPI_XBP_JOB_ADD_ABAP_STEP": steps[i]}
                            success, result = __salt__["sap_nwabap.call_fms"](
                                function_modules=function_modules, conn=conn
                            )
                            if not success:
                                msg = f"Could not add ABAP step {steps[i]['ABAP_PROGRAM_NAME']} to {name}"
                                log.error(msg)
                                ret["comment"] = msg
                                ret["result"] = False
                                return ret
                            if not __salt__["sap_nwabap.process_bapiret2"](
                                result["BAPI_XBP_JOB_ADD_ABAP_STEP"]["RETURN"]
                            ):
                                ret["comment"] = _get_bapiret2_messages(
                                    result["BAPI_XBP_JOB_ADD_ABAP_STEP"]["RETURN"]
                                )
                                ret["result"] = False
                                return ret
                            # update changes
                            if "STEPS" not in ret["changes"]["new"]:
                                ret["changes"]["new"]["STEPS"] = []
                            del steps[i]["JOBNAME"]
                            del steps[i]["JOBCOUNT"]
                            del steps[i]["EXTERNAL_USER_NAME"]
                        ret["changes"]["new"]["STEPS"].append(steps[i])
                    else:
                        log.debug(f"Calculating diffs for step #{i} of {name}")
                        # set correct names for existing job steps (field names differ between read <> modify)
                        job_steps[i] = _replace_human_readable(
                            job_steps[i], mapping=JOB_STEP_MODIFY_MAPPING, remove_unknown=True
                        )
                        diff = salt.utils.dictdiffer.deep_diff(job_steps[i], steps[i])
                        log.debug("Removing empty dictionaries")
                        diff = _clear_empty_dict(
                            diff, remove_none=False
                        )  # this will also remove empty "new" and "old" dicts
                        new = diff.get("new", {})
                        # because we're only interested in the changed fields, we remove unchanged stuff from "old"
                        old = _extract_changed_from_dict(new, diff.get("old", {}))
                        if new:
                            new["JOBNAME"] = name
                            new["JOBCOUNT"] = job_count
                            new["EXTERNAL_USER_NAME"] = session_id
                            new["STEP_NUMBER"] = i + 1
                            # we ALWAYS need the program name, even if it doesn't change
                            if "ABAP_PROGRAM_NAME" not in new:
                                rm_prog = False
                                new["ABAP_PROGRAM_NAME"] = steps[i]["ABAP_PROGRAM_NAME"]
                            else:
                                rm_prog = True
                            if not __opts__["test"]:
                                function_modules = {"BAPI_XBP_JOB_ABAP_STEP_MODIFY": new}
                                success, result = __salt__["sap_nwabap.call_fms"](
                                    function_modules=function_modules, conn=conn
                                )
                                if not success:
                                    msg = f"Could not modify ABAP step #{i} of {name}"
                                    log.error(msg)
                                    ret["comment"] = msg
                                    ret["result"] = False
                                    return ret
                                if not __salt__["sap_nwabap.process_bapiret2"](
                                    result["BAPI_XBP_JOB_ABAP_STEP_MODIFY"]["RETURN"]
                                ):
                                    ret["comment"] = _get_bapiret2_messages(
                                        result["BAPI_XBP_JOB_ABAP_STEP_MODIFY"]["RETURN"]
                                    )
                                    ret["result"] = False
                                    return ret
                            # update changes
                            del new["JOBNAME"]
                            del new["JOBCOUNT"]
                            del new["EXTERNAL_USER_NAME"]
                            if rm_prog:
                                del new["ABAP_PROGRAM_NAME"]
                            if "STEPS" not in ret["changes"]["old"]:
                                ret["changes"]["old"]["STEPS"] = []
                            if "STEPS" not in ret["changes"]["new"]:
                                ret["changes"]["new"]["STEPS"] = []
                            ret["changes"]["old"]["STEPS"].append(old)
                            ret["changes"]["new"]["STEPS"].append(new)
                log.debug(f"Checking if there are more then {i} steps defined for {name}")
                if len(job_steps) > i + 1:
                    log.debug(f"There are more steps defined on the NetWeaver server for {name}")
                    for j in range(i + 1, len(job_steps)):
                        log.debug(f"Deleting step #{j+1} of {name}")
                        if not __opts__["test"]:
                            function_modules = {
                                "BAPI_XBP_MODIFY_JOB_STEP": {
                                    "JOBNAME": name,
                                    "JOBCOUNT": job_count,
                                    "EXTERNAL_USER_NAME": session_id,
                                    "STEP_NUM": j + 1,
                                    "DELETE": "X",
                                }
                            }
                            success, result = __salt__["sap_nwabap.call_fms"](
                                function_modules=function_modules, conn=conn
                            )
                            if not success:
                                msg = f"Could not remove ABAP step {j} from {name}"
                                log.error(msg)
                                ret["comment"] = msg
                                ret["result"] = False
                                return ret
                            if not __salt__["sap_nwabap.process_bapiret2"](
                                result["BAPI_XBP_MODIFY_JOB_STEP"]["RETURN"]
                            ):
                                ret["comment"] = _get_bapiret2_messages(
                                    result["BAPI_XBP_MODIFY_JOB_STEP"]["RETURN"]
                                )
                                ret["result"] = False
                                return ret
                        # update changes
                        if "STEPS" not in ret["changes"]["old"]:
                            ret["changes"]["old"]["STEPS"] = []
                        ret["changes"]["old"]["STEPS"].append({"STEPNUMBER": j + 1})
            else:
                log.debug(f"Job {name} does not exist, creating")
                # because the inital job_open and all subsequent steps are bound together by the returned JOBCOUNT,
                # this is one big section
                if not __opts__["test"]:
                    function_modules = {
                        "BAPI_XBP_JOB_OPEN": {
                            "JOBNAME": name,
                            "EXTERNAL_USER_NAME": session_id,
                        }
                    }
                    success, result = __salt__["sap_nwabap.call_fms"](
                        function_modules=function_modules, conn=conn
                    )
                    if not success:
                        msg = f"Could not open job definition for {name}"
                        log.error(msg)
                        ret["comment"] = msg
                        ret["result"] = False
                        return ret
                    if not __salt__["sap_nwabap.process_bapiret2"](
                        result["BAPI_XBP_JOB_OPEN"]["RETURN"]
                    ):
                        ret["comment"] = _get_bapiret2_messages(
                            result["BAPI_XBP_JOB_OPEN"]["RETURN"]
                        )
                        ret["result"] = False
                        return ret
                    job_count = result["BAPI_XBP_JOB_OPEN"]["JOBCOUNT"]

                    # we need to add steps before we modify the header, otherwise we will get the error
                    # "Job has no steps"
                    log.debug(f"Adding steps to {name}")
                    for step in steps:
                        step["JOBNAME"] = name
                        step["JOBCOUNT"] = job_count
                        step["EXTERNAL_USER_NAME"] = session_id
                        function_modules = {"BAPI_XBP_JOB_ADD_ABAP_STEP": step}
                        success, result = __salt__["sap_nwabap.call_fms"](
                            function_modules=function_modules, conn=conn
                        )
                        if not success:
                            msg = f"Could not add ABAP step {step['ABAP_PROGRAM_NAME']} to {name}"
                            log.error(msg)
                            ret["comment"] = msg
                            ret["result"] = False
                            return ret
                        if not __salt__["sap_nwabap.process_bapiret2"](
                            result["BAPI_XBP_JOB_ADD_ABAP_STEP"]["RETURN"]
                        ):
                            ret["comment"] = _get_bapiret2_messages(
                                result["BAPI_XBP_JOB_ADD_ABAP_STEP"]["RETURN"]
                            )
                            ret["result"] = False
                            return ret

                    log.debug("Modifying header")
                    # because this is initial, we can just change everything
                    mask = {
                        "STARTCOND": "X",
                        "RECIPLNT": "X",
                        "THOST": "X",
                        "TSERVER": "X",
                        "TSRVGRP": "X",
                    }
                    function_modules = {
                        "BAPI_XBP_JOB_HEADER_MODIFY": {
                            "JOBNAME": name,
                            "JOBCOUNT": job_count,
                            "EXTERNAL_USER_NAME": session_id,
                            "JOB_HEADER": header,
                            "JOBCLASS": jobclass,
                            "MASK": mask,
                        }
                    }
                    success, result = __salt__["sap_nwabap.call_fms"](
                        function_modules=function_modules, conn=conn
                    )
                    if not success:
                        msg = f"Could not modify header for {name}"
                        log.error(msg)
                        ret["comment"] = msg
                        ret["result"] = False
                        return ret
                    if not __salt__["sap_nwabap.process_bapiret2"](
                        result["BAPI_XBP_JOB_HEADER_MODIFY"]["RETURN"]
                    ):
                        ret["comment"] = _get_bapiret2_messages(
                            result["BAPI_XBP_JOB_HEADER_MODIFY"]["RETURN"]
                        )
                        ret["result"] = False
                        return ret

                    log.debug("Closing job definition")
                    function_modules = {
                        "BAPI_XBP_JOB_CLOSE": {
                            "JOBNAME": name,
                            "JOBCOUNT": job_count,
                            "EXTERNAL_USER_NAME": session_id,
                        }
                    }
                    success, result = __salt__["sap_nwabap.call_fms"](
                        function_modules=function_modules, conn=conn
                    )
                    if not success:
                        msg = f"Could not close job definition for {name}"
                        log.error(msg)
                        ret["comment"] = msg
                        ret["result"] = False
                        return ret
                    if not __salt__["sap_nwabap.process_bapiret2"](
                        result["BAPI_XBP_JOB_CLOSE"]["RETURN"]
                    ):
                        ret["comment"] = _get_bapiret2_messages(
                            result["BAPI_XBP_JOB_CLOSE"]["RETURN"]
                        )
                        ret["result"] = False
                        return ret
                else:
                    job_count = "UNKNOWN"
                ret["changes"] = {
                    "old": None,
                    "new": {
                        "JOBNAME": name,
                        "JOBCOUNT": job_count,
                        "HEADER": header,
                        "STEPS": steps,
                    },
                }
        except Exception as exc:  # pylint: disable=broad-except
            # handle exception
            log.exception(exc)
            raise
        finally:
            log.debug("Logging off")
            function_modules = {
                "BAPI_XMI_LOGOFF": {
                    "INTERFACE": "XBP",
                }
            }
            success, result = __salt__["sap_nwabap.call_fms"](
                function_modules=function_modules, conn=conn
            )
            if not success:
                msg = "Could not log off"
                log.error(msg)
                ret["comment"] = msg
                ret["result"] = False
                return ret  # pylint: disable=lost-exception
            if not __salt__["sap_nwabap.process_bapiret2"](result["BAPI_XMI_LOGOFF"]["RETURN"]):
                ret["comment"] = _get_bapiret2_messages(result["BAPI_XMI_LOGOFF"]["RETURN"])
                ret["result"] = False
                return ret  # pylint: disable=lost-exception
    if not ret["changes"].get("new", None) and not ret["changes"].get("old", None):
        ret["changes"] = {}
        ret["comment"] = "No changes required"
    elif __opts__["test"]:
        ret["comment"] = f"Would have maintained job {name}"
    else:
        ret["comment"] = f"Maintained job {name}"
    ret["result"] = True if (not __opts__["test"] or not ret["changes"]) else None
    return ret


# pylint: disable=unused-argument
def job_absent(
    name,
    sid,
    client,
    message_server_host,
    message_server_port,
    logon_group,
    username,
    password,
    **kwargs,
):
    """
    Ensure that a job is absent in the system.

    name
        Name of the job.

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

    Example:

    .. code-block:: jinja

        SLD job SAP_SLD_DATA_COLLECT is absent on S4H:
          sap_nwabap.job_present:
            - name: SAP_SLD_DATA_COLLECT
            - sid: S4H
            - client: "000"
            - message_server_host: s4h
            - message_server_port: 3600
            - logon_group: SPACE
            - username: SALT
            - password: __slot__:salt:vault.read_secret(path="nwabap/S4H/000", key="SALT")
    """
    log.debug("Running function")
    name = name.upper()
    ret = {"name": name, "changes": {"old": {}, "new": {}}, "comment": "", "result": False}
    log.debug("Creating one connection for the lifetime of this state")
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
        log.debug("Logging in and retrieving session id")
        function_modules = {
            "BAPI_XMI_LOGON": {
                "EXTCOMPANY": "SAPUCC",
                "EXTPRODUCT": "PYTHON",
                "INTERFACE": "XBP",
                "VERSION": "2.0",
            }
        }
        success, result = __salt__["sap_nwabap.call_fms"](
            function_modules=function_modules, conn=conn
        )
        if not success:
            msg = "Could not logon"
            log.error(msg)
            ret["comment"] = msg
            ret["result"] = False
            return ret
        if not __salt__["sap_nwabap.process_bapiret2"](result["BAPI_XMI_LOGON"]["RETURN"]):
            ret["comment"] = _get_bapiret2_messages(result["BAPI_XMI_LOGON"]["RETURN"])
            ret["result"] = False
            return ret
        session_id = result["BAPI_XMI_LOGON"]["SESSIONID"]
        try:
            log.debug(f"Finding job {name}")
            function_modules = {
                "BAPI_XBP_JOB_SELECT": {
                    "JOB_SELECT_PARAM": {
                        "JOBNAME": name,
                        "USERNAME": "*",  # required for BAPI_XBP_JOB_SELECT to work
                        "PRELIM": "X",
                        "SCHEDUL": "X",
                    },
                    "EXTERNAL_USER_NAME": session_id,
                }
            }
            success, result = __salt__["sap_nwabap.call_fms"](
                function_modules=function_modules, conn=conn
            )
            if not success:
                msg = f"Could not retrieve data for {name}"
                log.error(msg)
                ret["comment"] = msg
                ret["result"] = False
                return ret
            if not __salt__["sap_nwabap.process_bapiret2"](result["BAPI_XBP_JOB_SELECT"]["RETURN"]):
                ret["comment"] = _get_bapiret2_messages(result["BAPI_XBP_JOB_SELECT"]["RETURN"])
                ret["result"] = False
                return ret

            if result["BAPI_XBP_JOB_SELECT"]["SELECTED_JOBS"]:
                if len(result["BAPI_XBP_JOB_SELECT"]["SELECTED_JOBS"]) > 1:
                    msg = f"There is more then 1 job with the name {name}, only deleting 1"
                    log.warning(msg)
                    ret["warnings"] = [msg]
                job = result["BAPI_XBP_JOB_SELECT"]["SELECTED_JOBS"][0]
                log.debug(f"Job {job['JOBNAME']} exists with id {job['JOBCOUNT']}, removing")
                if not __opts__["test"]:
                    function_modules = {
                        "BAPI_XBP_JOB_DELETE": {
                            "JOBNAME": job["JOBNAME"],
                            "JOBCOUNT": job["JOBCOUNT"],
                            "EXTERNAL_USER_NAME": session_id,
                        }
                    }
                    success, result = __salt__["sap_nwabap.call_fms"](
                        function_modules=function_modules, conn=conn
                    )
                    if not success:
                        msg = f"Could not delete {job['JOBNAME']}"
                        log.error(msg)
                        ret["comment"] = msg
                        ret["result"] = False
                        return ret
                    if not __salt__["sap_nwabap.process_bapiret2"](
                        result["BAPI_XBP_JOB_DELETE"]["RETURN"]
                    ):
                        ret["comment"] = _get_bapiret2_messages(
                            result["BAPI_XBP_JOB_DELETE"]["RETURN"]
                        )
                        ret["result"] = False
                        return ret
                ret["changes"] = {
                    "old": f"{job['JOBNAME']} - {job['JOBCOUNT']} deleted",
                    "new": None,
                }
        except Exception as exc:  # pylint: disable=broad-except
            log.exception(exc)
            raise
        finally:
            log.debug("Logging off")
            function_modules = {
                "BAPI_XMI_LOGOFF": {
                    "INTERFACE": "XBP",
                }
            }
            success, result = __salt__["sap_nwabap.call_fms"](
                function_modules=function_modules, conn=conn
            )
            if not success:
                msg = "Could not log off"
                log.error(msg)
                ret["comment"] = msg
                ret["result"] = False
                return ret  # pylint: disable=lost-exception
            if not __salt__["sap_nwabap.process_bapiret2"](result["BAPI_XMI_LOGOFF"]["RETURN"]):
                ret["comment"] = _get_bapiret2_messages(result["BAPI_XMI_LOGOFF"]["RETURN"])
                ret["result"] = False
                return ret  # pylint: disable=lost-exception
    if not ret["changes"].get("new", None) and not ret["changes"].get("old", None):
        ret["changes"] = {}
        ret["comment"] = "No changes required"
    elif __opts__["test"]:
        ret["comment"] = f"Would have removed job {name}"
    else:
        ret["comment"] = f"Removed job {name}"
    ret["result"] = True if (not __opts__["test"] or not ret["changes"]) else None
    return ret


# pylint: disable=unused-argument
def system_health_ok(
    name,
    check_from,
    client,
    message_server_host,
    message_server_port,
    logon_group,
    username,
    password,
    max_allowed_dumps=0,
    **kwargs,
):
    """
    Check the system health by checking:
        - Transaction ``SICK``
        - Short Dumps

    name
        SID of the SAP system.

    check_from
        Date from which on the system health should be checked (e.g. for log entries)
         in the format ``DDMMYYYY``, e.g. ``31129999`` or ``01012000``.

    max_allowed_dumps
        Maximum number of allowed short dumps (default: 0).

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

    Example:

    .. code-block:: jinja

        System healh is OK for SAP NetWeaver AS ABAP system S4H (ST22 / SICK):
          sap_nwabap.system_health_ok:
            - name: S4H
            - check_from: {{ None | strftime("%d%m%Y") }}  {# renders to current date, e.g. 31082022 #}
            - client: "000"
            - message_server_host: s4h
            - message_server_port: 3600
            - logon_group: SPACE
            - username: SALT
            - password: __slot__:salt:vault.read_secret(path="nwabap/S4H/000", key="SALT")

    .. note::
        This function does not implement ``__opts__["test"]`` since no data is changed.
    """
    log.debug("Running function")
    ret = {"name": name, "changes": {}, "comment": [], "result": False}

    log.debug("Creating one connection for the lifetime of this state")
    if isinstance(client, int):
        client = f"{client:03d}"
    abap_connection = {
        "mshost": message_server_host,
        "msserv": str(message_server_port),
        "sysid": name,
        "group": logon_group,
        "client": client,
        "user": username,
        "passwd": password,
        "lang": "EN",
    }
    with Connection(**abap_connection) as conn:

        log.debug("Checking for ABAP dumps")
        function_modules = {
            "/SDF/EWA_GET_ABAP_DUMPS": {
                "BEDATUM": _convert_date(check_from),
            }
        }
        success, result = __salt__["sap_nwabap.call_fms"](
            function_modules=function_modules, conn=conn
        )
        if not success:
            msg = "Could not retrieve ABAP dumps"
            log.error(msg)
            ret["comment"] = msg
            ret["changes"] = {}
            ret["result"] = False
            return ret
        num_dumps = len(result["/SDF/EWA_GET_ABAP_DUMPS"]["I_SNAP_ERROR_DAY"])
        if num_dumps > max_allowed_dumps:
            msg = f"ST22: System contains {num_dumps} short dumps"
            ret["comment"].append(msg)

        log.debug("Checking for SICK")
        function_modules = {
            "INST_EXECUTE_REPORT": {"PROGRAM": "RSICC000"}  # report behind transaction SICK
        }
        success, result = __salt__["sap_nwabap.call_fms"](
            function_modules=function_modules, conn=conn
        )
        if not success:
            msg = "Could not run transaction SICK"
            log.error(msg)
            ret["comment"] = msg
            ret["changes"] = {}
            ret["result"] = False
            return ret
        output = [e["ZEILE"].strip("|") for e in result["INST_EXECUTE_REPORT"]["OUTPUT_TAB"]]
        errors = True
        for line in output:
            if "no errors reported" in line:
                errors = False
                break
        if errors:
            log.error("SICK reported errors:")
            for line in output:
                log.error(line)
                ret["comment"].append(f"SICK: {output}")

    if ret["comment"]:
        ret["result"] = False
    else:
        ret["comment"] = "System health OK"
        ret["result"] = True
    return ret
