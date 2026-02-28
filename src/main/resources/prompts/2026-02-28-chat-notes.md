# ProjectAI Chat Notes - 2026-02-28

## Summary
This session focused on a major UX and feature upgrade of the MDRM console, including landing-page search/discovery, authentication, bookmarks/favorites, dark mode improvements, and visual refresh.

## Work Completed
- Refactored the main app flow so Discovery is the default landing page.
- Moved search/discovery behavior out of the old MDRM-first flow.
- Added auth capabilities:
  - Sign up
  - Login
  - Logout
  - Reset password (email + new password)
  - CSRF protection for auth/bookmark POST/PATCH/DELETE flows
- Added bookmark/favorites capabilities:
  - Save item to default favorites
  - Create group at save time
  - Browse bookmark groups and items
  - Move and remove bookmarks
- Redesigned favorites UX:
  - Removed old right-side favorites panel from discovery
  - Added dedicated Bookmarks menu/page
  - Added row-level star action in search grid
- Reworked search results:
  - Reduced visible columns to MDRM Code, Description, Reporting Form
  - Description truncation with tooltip for long text
  - Results hidden until search
  - Added AG Grid for discovery results
  - Added pagination (default page size 20)
  - Added quick filter search box
  - Added favorite-first ordering for initial result render
  - Added sortable Favorite column
- Improved theme behavior:
  - Fixed dark mode readability issues
  - Added robust light/dark switching behavior
  - Synced Tailwind dark class and existing theme variables
- Applied professional visual refresh:
  - Updated header/menu style with icon capsules
  - Added colorful, theme-safe nav icons
  - Propagated the same nav style to `mdrm.html`
  - Removed dedicated top-level MDRM menu item
- Updated dashboard KPIs:
  - Unique Reports count
  - MDRMs count
  - Current View
  - Same-row layout on desktop
  - Formatted counts with locale separators

## Key Backend Changes
- Added auth module and services.
- Added bookmark module and services.
- Added CSRF service + validation checks.
- Added reset-password endpoint.
- Fixed bookmark duplicate-check SQL for PostgreSQL null handling:
  - Replaced nullable `? IS NULL` pattern with `IS NOT DISTINCT FROM CAST(? AS BIGINT)`.

## Notes
- Temporary password was set for `43.lalit@gmail.com` during this session using a direct DB update.
- Redirect pages (`mdrm-ui.html`, `mdrm-reporting.html`) were not redesigned since they redirect to `/`.
