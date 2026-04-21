# Book2go Database Documentation

## Overview

Book2go is a booking and scheduling platform used by **Cycling Without Age** — a global movement that offers elderly and people with limited mobility free bike rides on trishaws (rickshaws). The database powers the scheduling, user management, and community coordination behind these trips.

The database is PostgreSQL, originally hosted on Supabase.

---

## Tables

### 1. `user`
Represents all registered users in the system (pilots, coordinators, managers, helpers, etc.).

| Column | Type | Description |
|---|---|---|
| id | uuid (PK) | Unique user identifier |
| first_name | text | User's first name |
| last_name | text | User's last name |
| email | text | Email address |
| phone | text | Phone number |
| phone_country | text | Phone country code |
| country | text | Country (e.g. "dk", "us") |
| language | text | Preferred language (default: "en") |
| zip | text | Zip/postal code |
| gender | text | Gender |
| description | text | Profile description |
| picture | text | Profile picture URL |
| birthdate | timestamptz | Date of birth |
| notifications | boolean | Whether notifications are enabled |
| deletion_request | timestamptz | When user requested account deletion |
| auth_id | uuid | Link to Supabase auth (not included in export) |
| firestore_id | text | Legacy ID from previous Firestore database |
| created_at | timestamptz | When the user was created |
| updated_at | timestamptz | Last update timestamp |

---

### 2. `account`
Extended account settings per user. One-to-one relationship with `user`.

| Column | Type | Description |
|---|---|---|
| id | bigint (PK) | Auto-increment ID |
| user_id | uuid (FK → user.id) | The associated user |
| is_admin | boolean | Whether user is a global admin |
| country_admin | jsonb | Array of country codes where user is country admin (e.g. `["dk","se"]`) |
| is_active | boolean | Whether the account is active |
| is_trained_pilot | boolean | Whether user is a trained pilot (across all communities) |
| gdpr_consent | timestamptz | When GDPR consent was given |

---

### 3. `community`
A community represents a **location/chapter** — typically a care home, activity center, or local group that organizes trishaw rides.

| Column | Type | Description |
|---|---|---|
| id | bigint (PK) | Community identifier |
| name | text | Community name |
| description | text | Public description |
| description_private | text | Internal/private notes |
| email | text | Contact email |
| phone | text | Contact phone |
| picture | text | Cover image URL |
| country | text | Country code |
| region | text | Region/state |
| municipality | text | Municipality |
| city | text | City |
| zip | text | Postal code |
| road | text | Street name |
| number | text | Street number |
| public_address | text | Full address for display |
| latitude | numeric | GPS latitude |
| longitude | numeric | GPS longitude |
| timezone | text | Timezone identifier |
| show_urgent_message | boolean | Whether to show an urgent banner |
| description_urgent_title | text | Urgent message title |
| description_urgent_message | text | Urgent message body |
| firestore_id | text | Legacy Firestore ID |
| created_at | timestamptz | Created timestamp |
| updated_at | timestamptz | Last update timestamp |

---

### 4. `community_config`
Configuration settings per community. One-to-one with `community`.

| Column | Type | Description |
|---|---|---|
| id | bigint (PK) | Auto-increment ID |
| community_id | bigint (FK → community.id) | The community |
| trip_requires_confirmation | boolean | If true, trips must be confirmed by a coordinator before they are approved |
| is_care_center | boolean | Whether the community is a care center |
| is_home_residence | boolean | Whether the community is a home/private residence |

---

### 5. `community_subscription`
Subscription/activation state per community. One-to-one with `community`.

| Column | Type | Description |
|---|---|---|
| id | bigint (PK) | Auto-increment ID |
| community_id | bigint (FK → community.id) | The community |
| is_subscribed | boolean | Whether the community has an active subscription |
| is_active | boolean | Whether the community is active/visible |

---

### 6. `roles`
Lookup table for user roles within communities.

| Column | Type | Description |
|---|---|---|
| id | bigint (PK) | Role ID |
| name | text | Role name |

**Known roles:**
- `coordinator` — Manages trips and community operations
- `pilot` — Trained and approved trishaw pilot
- `trained_pilot` — Pilot with completed training
- `untrained_pilot` — Pilot awaiting training
- `helper` — Assists with rides and community tasks
- `manager` — Administrative role for the community

---

### 7. `community_users`
**Junction table** linking users to communities with a specific role. A user can have multiple roles across multiple communities.

| Column | Type | Description |
|---|---|---|
| id | bigint (PK) | Auto-increment ID |
| user_id | uuid (FK → user.id) | The user |
| community_id | bigint (FK → community.id) | The community |
| role_id | bigint (FK → roles.id) | The assigned role |

**Unique constraint:** One entry per (user_id, community_id, role_id) combination.

---

### 8. `resource`
Represents a **trishaw** (rickshaw bicycle) or other bookable vehicle/equipment.

| Column | Type | Description |
|---|---|---|
| id | bigint (PK) | Resource ID |
| name | text | Resource name (e.g. "Trishaw 1") |
| description | text | Description |
| type | text | Resource type |
| brand | text | Manufacturer/brand |
| picture | text | Image URL |
| status | text | Current status (e.g. "inoperative") |
| is_active | boolean | Whether the resource is active |
| serial_number | text | Serial number |
| battery_name | text | Battery model/name |
| battery_serial_number | text | Battery serial number |
| reperation_from_date | timestamptz | Repair period start |
| reperation_to_date | timestamptz | Repair period end |
| reperation_message | text | Repair notes |
| firestore_id | text | Legacy Firestore ID |
| created_at | timestamptz | Created timestamp |
| updated_at | timestamptz | Last update timestamp |

---

### 9. `community_resources`
**Junction table** linking resources (trishaws) to communities. A resource can belong to multiple communities.

| Column | Type | Description |
|---|---|---|
| id | bigint (PK) | Auto-increment ID |
| resource_id | bigint (FK → resource.id) | The resource |
| community_id | bigint (FK → community.id) | The community |

---

### 10. `trip`
A **trip** represents a single scheduled trishaw ride — the core entity of the booking system.

| Column | Type | Description |
|---|---|---|
| id | bigint (PK) | Trip ID |
| start | timestamptz | Scheduled start time |
| end | timestamptz | Scheduled end time |
| caption | text | Trip description/title |
| status | text | Trip status: `pending`, `approved`, or `cancelled` |
| community_id | bigint (FK → community.id) | The community where the trip takes place |
| pilot_id | uuid (FK → user.id) | The pilot assigned to drive |
| resource_id | bigint (FK → resource.id) | The trishaw/resource used |
| created_by_id | uuid (FK → user.id) | Who created the trip |
| created_by_role_id | bigint (FK → roles.id) | The role of the person who created the trip |
| is_intern_trip | boolean | Whether this is an internal/practice trip |
| firestore_id | text | Legacy Firestore ID |
| created_at | timestamptz | Created timestamp |
| updated_at | timestamptz | Last update timestamp |

---

### 11. `trip_participants`
Passengers (typically elderly residents) on a trip.

| Column | Type | Description |
|---|---|---|
| id | bigint (PK) | Auto-increment ID |
| trip_id | bigint (FK → trip.id) | The trip |
| user_id | uuid (FK → user.id) | User reference (if registered) |
| name | text | Participant name |
| email | text | Participant email |
| role | text | Participant role in the trip |

---

### 12. `trip_notes`
Notes or comments attached to trips.

| Column | Type | Description |
|---|---|---|
| id | bigint (PK) | Auto-increment ID |
| trip_id | bigint (FK → trip.id) | The trip |
| user_id | uuid (FK → user.id) | Who wrote the note |
| text | text | Note content |
| timestamp | timestamptz | When the note was written |

---

## Entity Relationship Diagram

```
                    ┌──────────────┐
                    │    roles     │
                    │──────────────│
                    │ id (PK)      │
                    │ name         │
                    └──────┬───────┘
                           │
                           │ role_id (FK)
                           │
┌──────────┐    ┌──────────┴───────────┐    ┌───────────────┐
│   user   │───▶│   community_users    │◀───│   community   │
│──────────│    │──────────────────────│    │───────────────│
│ id (PK)  │    │ user_id (FK)         │    │ id (PK)       │
│ name     │    │ community_id (FK)    │    │ name          │
│ email    │    │ role_id (FK)         │    │ country       │
│ phone    │    └──────────────────────┘    │ city          │
│ country  │                                │ lat/lng       │
└────┬─────┘                                └───┬───┬───────┘
     │                                          │   │
     │ user_id (FK)                             │   │ community_id (FK)
     │                                          │   │
┌────┴─────┐     ┌─────────────────┐           │  ┌┴──────────────────┐
│ account  │     │    resource     │           │  │ community_config  │
│──────────│     │─────────────────│           │  │ community_subscr. │
│ user_id  │     │ id (PK)         │           │  └───────────────────┘
│ is_admin │     │ name            │           │
│ country_ │     │ type/brand      │           │ community_id (FK)
│   admin  │     │ status          │           │
└──────────┘     └───┬─────────────┘    ┌──────┴──────────────┐
                     │                  │ community_resources │
                     │ resource_id (FK) │─────────────────────│
                     └──────────────────│ resource_id (FK)    │
                                        │ community_id (FK)   │
                                        └─────────────────────┘

┌──────────────────────────────────────────────────────────┐
│                         trip                              │
│──────────────────────────────────────────────────────────│
│ id (PK)                                                   │
│ start / end                   (timestamps)                │
│ status                        (pending/approved/cancelled)│
│ community_id (FK → community)                             │
│ pilot_id (FK → user)                                      │
│ resource_id (FK → resource)                               │
│ created_by_id (FK → user)                                 │
└────────┬──────────────────────────────┬──────────────────┘
         │                              │
         │ trip_id (FK)                 │ trip_id (FK)
         │                              │
┌────────┴────────────┐     ┌──────────┴──────────┐
│ trip_participants   │     │    trip_notes        │
│─────────────────────│     │─────────────────────│
│ name / email        │     │ text                 │
│ user_id (FK → user) │     │ user_id (FK → user)  │
│ role                │     │ timestamp            │
└─────────────────────┘     └─────────────────────┘
```

---

## Key Relationships Summary

1. **User ↔ Community**: Many-to-many through `community_users` (with role)
2. **Resource ↔ Community**: Many-to-many through `community_resources`
3. **Trip → Community**: Each trip belongs to one community
4. **Trip → User (pilot)**: Each trip has one pilot
5. **Trip → Resource**: Each trip may use one trishaw
6. **Trip ↔ Participants**: One-to-many (passengers on a trip)
7. **Trip ↔ Notes**: One-to-many (comments on a trip)
8. **User ↔ Account**: One-to-one (extended settings)
9. **Community ↔ Config/Subscription**: One-to-one

---

## Data Privacy Notice

This dataset contains **personally identifiable information (PII)** including names, email addresses, and phone numbers. This data is shared for internal academic analysis only.

**Requirements:**
- Do NOT use names, emails, phone numbers, or other personal identifiers in any report or publication
- Do NOT take screenshots containing personal data
- Do NOT share this data outside the authorized research group
- Treat all personal data in accordance with GDPR regulations
