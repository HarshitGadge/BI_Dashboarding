# Metric definitions

| Metric | Definition | Notes |
|---|---|---|
| Ticket | One member support request, from creation to resolution | Grain of `dashboard_data` |
| Resolution time | `closed_at − created_at`, in minutes | Wall-clock time, not business hours |
| SLA target | High: 60 min · Medium: 240 min (4 h) · Low: 1,440 min (24 h) | Set by priority at ticket creation |
| SLA met | 1 if resolution time ≤ the ticket's SLA target, else 0 | |
| SLA compliance % | Share of tickets with SLA met | Reported per week, team, agent, channel × priority |
| CSAT | Survey rating 1–5 from the member after the ticket closes | Only for tickets with a survey response |
| % satisfied | Share of survey responses rated 4 or 5 | |
| Survey response % | Share of tickets with a CSAT response | Low response rates make CSAT noisy at agent level |
| Tenure (months) | Months between agent hire date and the end of the reporting period | |
