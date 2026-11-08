# Security policy

Report a vulnerability privately through GitHub's "Report a
vulnerability" button on this repository's Security tab. Please do not
open a public issue for it. You will get a reply within 7 days.

Scope: the code in this repository and the static dashboard built from
site/. The study's Airflow deployment is a set of local test processes
that the harness starts and stops; the harness talks to its API server
on 127.0.0.1:8080, so run it on a machine whose port 8080 is not
reachable from untrusted networks. No credential is stored in this
repository.
