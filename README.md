# denylist-automation

Tools for efficient analysis and processing of submissions to the [Helium Denylist](https://github.com/helium/denylist).

## Overview

Before a hotspot's rewards are actually denied on-chain, there are three major steps that must take place: 

1. The hotspot's address is submitted through a denylist [issue](https://github.com/denylist/issues).
2. Issues are reviewed and compiled into pull requests that propose updates to `denylist.csv`. 
3. A group of 6 anonymous signers review individual PR's. When a 3-of-6 multisig transaction is signed for a given PR, releases are automatically tagged through a [Github Action](https://github.com/helium/denylist/actions). At this point, validators will pick up the updated xorf filter and use it to deny PoC activity to addresses in `denylist.csv`.

<img height="500" src="assets/denylist-flowchart.png"/>

This project is focused on the second step, which represents the most significant bottleneck in the system. While technically, any user could submit a PR, in practice, this responsibility has been left to 1-2 (mostly 1). There are a few reasons for this:

- The sheer quantity of submissions is daunting (>7000 issues mention >90,000 individual hotspots).
- Reviewing issue submissions is extremely tedious. The issues themselves are highly variable in terms of the quality/quantity of evidence and even the formatting of the lists. There is no validation to ensure that submitters are listing valid hotspot addresses (and not names, links, or wallet hashes). 
- Trusted reviewers investigate all (or nearly all) individual hotspots using their own analysis tools, many of which involve time-intensive queries and lots of copying/pasting into multiple browser tabs. 
- Removals are almost impossible to verify, as once a hotspot is on the denylist, all of its PoC activity disappears. 
- Unfortunately, the denylist repo itself has become its own toxic subculture of abuse and doxxing attacks. Some bad actors will try to impersonate more productive users by using very similar usernames and avatars. This only adds another element of complication to the review process.
- Generating the PR's themselves is not technically trivial (deduping the list, generating the manifest).
- Perhaps most importantly, there is no direct incentive for any of this. Submitters and maintainers earn nothing for their hard work, creating a fragile and frustrating ecosystem - prolific submitters (rightfully) feel that the process is too slow, but it is critical to maintain a low false positive rate.

`denylist-automation` aims to make this part of the process a little bit easier by parsing the contents of the repo (issues, PR's) into a normalized database. We also run the analytics asynchronously and cache the results in S3 to minimize the latency during independent reviews. A crude-but-effective dashboard allows users to interact with this data in an intuitive way. 

<img height="400" src="assets/system-overview.png"/>

## Getting Started

**Note**: These instructions are based on a local deploy of BOTH the parsing service (`run.py`) and the dashboard UI (`app.py`). For a Heroku deploy of the dashboard ONLY, use [Config Vars](https://devcenter.heroku.com/articles/config-vars) to define the environment variables, rather than `.env`. 

**For BOTH services, start here:**
1. Clone the repo and make a copy of `.env.template` called `.env`.
2. Generate a [Github Personal Access Token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token) to ensure that you aren't rate-limited while scraping the [REST API](https://docs.github.com/en/rest) for issue/PR details. Paste your token (starts with `ghp_`) into `.env` for the `GITHUB_ACCESS_TOKEN` variable. 
3. You'll need access to a Postgres database populated by [`blockchain-etl`](https://github.com/helium/blockchain-etl) in order to run the analysis on individual hotspots. Since I use an SSH tunnel to access my db, the environment variables and connection method reflect that. However, if you use an alternative connection method (like a simple connection string), please open an [issue](https://github.com/evandiewald/denylist-automation/issues), and I'll be happy to add that functionality.

**For dashboard ONLY, start here:**
4. Create a separate Postgres database for the parsed details and provide the connection string as the value for `DENYLIST_DB_CONNECTION_STRING`. Make sure you have the appropriate permissions to create schema (for the migrations) and insert data. You can also specify an alternative schema from the default `public`.
5. Create an S3 bucket for the cached query results and provide the name under `S3_BUCKET`. Refer to [boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details on authentication. The `default` profile is used by default, but this can be changed if you have multiple profiles. 
6. Paste a free [Mapbox](https://www.mapbox.com/) token into `.env` under `MAPBOX_TOKEN`.
7. Install required packages via `pip install requirements.txt` (Ubuntu) or `pip install requirements-win.txt` (Windows). 

## Usage

**Backend**

Currently, data is populated/updated as a batch job of the `run.py` script - however, I'm hoping to use [Webhooks](https://docs.github.com/en/developers/webhooks-and-events/webhooks/webhook-events-and-payloads) from the `helium/denylist` repo to stream updates in real-time (this requires admin permissions to that repo).

Run the job with 

`python run.py`

The initial job will take the longest, as you'll retrieve all 7000+ issues from the repo, parse them for individual entries, and link them to PR's. For issues submitted in the past 14 days, the analytics reports will be run and cached in S3. For subsequent runs, only new reports will need to be generated, but we do still have to look through all the issues for any updates, like closures. Again, this will be optimized with webhooks.

In practice, I just use cronjobs to run the update job at a daily cadence. 

**Frontend**

The dashboard is built with Dash, and can be served with

`python app.py`

By default, it is hosted on port 8050.

