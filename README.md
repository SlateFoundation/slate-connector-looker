# slate-connector-looker
Synchronize users from Slate to Looker

## What gets pushed

### Permissions & Roles

We are currently adding the following groups/roles/custom attributes in Looker:

- Role: `Admin` -- Network hub users that have been manually promoted to Administrator or Developer in the hub. This is currently being done directly in the DB.
- Role: `Staff(explore)` -- Slate network site users that are Administrator or Developer's in their respective slate site.
- Role: `Staff(view)` -- Slate network site users that have Staff account level in their respective slate site. This likely needs to be expanded to include Teacher account level slate accounts.
- Group: `[School] Administrators` -- Slate network site users that are Admin/Dev in their respective slate sites.
- Group: `[School] Staff` -- Slate network site users that are Staff/Teacher in their respective slate sites.
- Group: `[School] Students` -- Slate network site users that are Students in their respective slate sites.

### Custom Attributes

- `school`: Set from Slate Network School record config. This is currently only editable directly from the DB. It is also currently only set during the sync workflow when the user has only one school association. This seems fine, however we will likely need to create another workflow for updating this value via a multi-school hub user workflow.
- `student_id`: Set from the slate network users StudentNumber in their respective slate instance.
