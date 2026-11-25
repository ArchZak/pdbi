# Bind a confirmed submission to its researcher and retry key

The form and wizard validate drafts through the same endpoint and submit only after confirmation. One key per confirmed click identifies one run for the verified Cognito issuer and subject. Record ownership atomically with the query and reject conflicting criteria. A retry can repair initial queue dispatch while an explicit resume controls recovery from a paused run.

Keep this ownership check with the run checkpoint so access control works independently of the account history index. Existing ownerless CLI runs remain internal.
