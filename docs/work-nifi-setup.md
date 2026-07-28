# Connecting niflow to a work NiFi (the unknown-server playbook)

This is the checklist for pointing niflow at a NiFi you didn't set up —
typically HTTPS on port 8443, running in Podman/Docker on some host, loaded
by other automation (Liquibase jobs, CI, etc.), with an auth mode nobody
remembers. Work through it top to bottom; at every step `niflow doctor`
tells you what's wrong and which key to set next.

**niflow itself is pure Python.** No Docker/Podman is needed to *run* it —
containers appear in this repo only to host disposable NiFis for testing.
At work: `pip install -e .` (plus `.[gui]` if you want the desktop helper),
fill in `.niflow.env`, done.

---

## 0. The 5-minute happy path

```bash
pip install -e .
cp .niflow.env.example .niflow.env        # then edit it
NIFLOW_NIFI_HOST=https://<host>:8443/nifi-api niflow doctor
```

The doctor runs six checks (config sanity → TLS → server auth mode → your
credentials → identity → canvas access) and each failure names the exact
`.niflow.env` key to fix. When it prints `All good`, every niflow command —
CLI, GUI, web GUI, library — connects the same way from then on.

## 1. Find the server

- The REST base is the UI URL with `/nifi` replaced by `/nifi-api`:
  `https://host:8443/nifi` → `https://host:8443/nifi-api`.
- If you can shell into the box running it:

  ```bash
  podman ps                                   # find the NiFi container + port mapping
  podman inspect <container> | grep -A5 Ports # host port -> container 8443
  ```

## 2. Determine the auth mode

Three signals, use whichever is easiest:

**a) `niflow doctor`** (preferred — it automates b and c):
- `TLS handshake: the server REQUIRES a client certificate` → mTLS, go to §4.
- `server supports username/password login` → token login, go to §3.
- `TLS trust: could not verify the server certificate` → you're connecting
  fine but need the CA cert (§5) or `NIFLOW_NIFI_VERIFY_SSL=false` to keep
  probing.

**b) Browser behavior** at `https://host:8443/nifi`:
- A NiFi **login form** → username/password (single-user or LDAP behind it).
- The browser **asks you to pick a certificate** (or you're let straight in
  because IT preinstalled one) → mTLS.
- Redirect to a company **SSO page** (Okta/Keycloak/ADFS) → OIDC. niflow
  does not speak OIDC; ask the admins for a service account (LDAP
  user/password) or a client certificate instead.

**c) Read the server's own config** (needs shell access to the container):

```bash
podman exec <container> grep -E \
  'nifi.security.(user.login.identity.provider|user.oidc.discovery.url|needClientAuth|keystore=|truststore=)' \
  /opt/nifi/nifi-current/conf/nifi.properties
```

| Property | Meaning |
|---|---|
| `nifi.security.user.login.identity.provider=single-user-provider` | username/password (credentials in `login-identity-providers.xml`) |
| `...login.identity.provider=ldap-provider` | username/password, checked against LDAP — use your directory credentials |
| `...login.identity.provider=` (empty) and no OIDC URL | no login endpoint → identity must come from a client certificate |
| `nifi.security.user.oidc.discovery.url=https://…` | SSO/OIDC (niflow: use a service account instead) |
| `nifi.security.needClientAuth=true` / `WANT` | server accepts (or demands) mTLS |

Also useful: `podman exec <container> cat /opt/nifi/nifi-current/conf/login-identity-providers.xml`
shows the single-user username (never the real password — it's hashed).

Raw curl equivalents, if you'd rather probe by hand:

```bash
curl -k https://host:8443/nifi-api/access/config   # {"config":{"supportsLogin":true|false}}
curl -k https://host:8443/nifi-api/flow/about      # 401 = auth required, 200 = anonymous read
```

## 3. Username/password setup

```ini
# .niflow.env
NIFLOW_NIFI_HOST=https://host:8443/nifi-api
NIFLOW_NIFI_USER=your-user
NIFLOW_NIFI_PASSWORD=your-password
```

Works for single-user auth AND LDAP — both sit behind `POST /access/token`.
This file can contain a real password: it is git-ignored here; keep it that
way in the work repo too (`echo .niflow.env >> .gitignore`).

## 4. Client-certificate (mTLS) setup

You need a certificate **issued for you** by whatever CA the server trusts —
ask the NiFi admins; you'll usually receive a `.p12`/`.pfx` bundle. niflow
(via Python `requests`) wants PEM, so convert once:

```bash
openssl pkcs12 -in you.p12 -clcerts -nokeys -out me.cert.pem   # certificate
openssl pkcs12 -in you.p12 -nocerts -nodes  -out me.key.pem    # private key (unencrypted)
chmod 600 me.key.pem
```

```ini
# .niflow.env
NIFLOW_NIFI_HOST=https://host:8443/nifi-api
NIFLOW_NIFI_CLIENT_CERT=/home/you/certs/me.cert.pem
NIFLOW_NIFI_CLIENT_KEY=/home/you/certs/me.key.pem
```

With a cert configured, niflow skips token login entirely — the certificate
is the identity. If the doctor says `authenticated but ... rejected/
unauthorized`, the cert works at the TLS layer but NiFi doesn't know the
identity: an admin must add your certificate DN (shown in the error / in
`openssl x509 -in me.cert.pem -noout -subject`) as a user with policies.

Gotcha worth knowing (verified on 1.24): even NiFi's *initial admin*
identity only gets tenant/policy/controller rights — **view/modify on the
process groups is a separate policy** an admin grants per group (or on root).
`niflow doctor` green through "identity" but a 403 like `No applicable
policies could be found` on push means exactly this.

## 5. Trusting the server certificate

Best practice at work is real verification instead of `VERIFY_SSL=false`.
Grab the server/CA certificate once:

```bash
openssl s_client -connect host:8443 -showcerts </dev/null 2>/dev/null \
  | awk '/BEGIN CERT/,/END CERT/' > nifi-ca.pem
```

```ini
NIFLOW_NIFI_CA_BUNDLE=/home/you/certs/nifi-ca.pem
```

(When `CA_BUNDLE` is set, `VERIFY_SSL` is ignored.)

## 6. Verify, then work

```bash
niflow doctor          # all ✓
niflow list            # see the canvas tree
niflow pull "Top Level Group" -o flows/top.py
niflow plan flows/top.py
```

Notes for a Liquibase/CI-managed instance:

- **Pulling is always safe** — it's read-only, whatever else manages the flow.
- Before pushing, check whether the group you're targeting is owned by other
  automation. `niflow plan` is read-only too, so plan freely; apply
  (`push --update`) only to groups that are yours.
- Groups under NiFi Registry version control are handled: an in-place push
  preserves the registry link and shows up as local changes.

## Troubleshooting quick table

| Doctor says | Do |
|---|---|
| `reachability: cannot reach` | wrong host/port, container down (`podman ps`), or VPN/proxy in the way |
| `TLS trust` | §5 (CA bundle), or `NIFLOW_NIFI_VERIFY_SSL=false` while investigating |
| `TLS handshake ... REQUIRES a client certificate` | §4 |
| `auth mismatch: no login endpoint` | server is cert-auth; §4 |
| `authentication: wrong username/password` | try your LDAP/directory credentials; confirm with admins |
| `authentication: certificate ... rejected` | admin must authorize your cert DN |
| `canvas access: lacks policies` | you're authenticated but need view/modify policies on the groups you care about |
| Login page is SSO (Okta etc.) | ask for a service account or a client cert — OIDC is not supported |
