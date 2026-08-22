# sharyt — build output

Generated artefacts only. The source lives in a private repository; this exists
because the hosting platform clones over anonymous HTTPS and cannot read one.

Nothing here is written by hand — `npm run bundle` produces it.

Publishing the build output is deliberate rather than incidental. The security
of this application does not rest on the code being unreadable: tokens are
stored as hashes, tenant isolation is enforced by Postgres row-level security,
and every payment webhook is verified against a per-restaurant secret. All of
that holds whether or not somebody can read `server.js`.
