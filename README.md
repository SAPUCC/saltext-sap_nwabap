# SaltStack SAP NetWeaver AS ABAP extension
This SaltStack extensions allows managing SAP NetWeaver AS ABAP systems over `pyrfc` resp. the SAP NW RFC SDK.

**THIS PROJECT IS NOT ASSOCIATED WITH SAP IN ANY WAY**

## Installation
Run the following to install the SaltStack SAP NetWeaver AS ABAP extention:
```bash
salt-call pip.install saltext.sap-nwabap
```
Note that this will install `pyrfc` as a dependency which requires the correct setup of the SAP NW RFC SDK as a
prerequisite. See https://github.com/SAP/PyRFC for more information.

In order to fulfill the shared library dependencies, the `salt-minion` requires to have the necessary environment
variables `LD_LIBRARY_PATH` and `SAPNWRFC_HOME` set. This can be achieved by modifying the systemd service of the
`salt-minion` (this can also be used for the installation of `pyrfc`).

_Note_: Due to the implementation of `salt-call`, environment variables set in the systemd service of the salt minion
are not recognized. Either use the CLI tool `salt` or [`pepper`](https://github.com/saltstack/pepper) or set the
environment variables prior to the execution on the local shell.

Keep in mind that this package must be installed on every minion that should utilize the states and execution modules.

Alternatively, you can add this repository directly over gitfs
```yaml
gitfs_remotes:
  - https://github.com/SAPUCC/saltext-sap_nwabap.git:
    - root: src/saltext/sap_nwabap
```
In order to enable this, logical links under `src/saltext/sap_nwabap/` from `_<dir_type>` (where the code lives) to `<dir_type>` have been placed, e.g. `_modules` -> `modules`. This will double the source data during build, but:
 * `_modules` is required for integrating the repo over gitfs
 * `modules` is required for the salt loader to find the modules / states

## Usage
A state using the SAP NetWeaver AS ABAP extension looks like this:
```jinja
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
```

## Docs
See https://saltext-sap-nwabap.readthedocs.io/ for the documentation.

## Contributing
We would love to see your contribution to this project. Please refer to `CONTRIBUTING.md` for further details.

## License
This project is licensed under GPLv3. See `LICENSE.md` for the license text and `COPYRIGHT.md` for the general copyright notice.
