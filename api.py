from marko import parser
import marko
import os
import re
from dotenv import load_dotenv
import pandas as pd
import requests
from typing import Optional


load_dotenv()


def parse_body(p: parser.Parser, body: str):

    elem = p.parse(body)

    try:
        parsed_elem = {}
        k, v = None, []
        for e in elem.children:
            if type(e) is marko.block.Heading:
                if k and v:
                    parsed_elem[k] = v

                if type(e.children[0].children) is str:
                    header = e.children[0].children
                else:
                    header = e.children[0].children[0].children

                k = header.lower().replace(' ', '_')
                k = re.sub("[^a-z_[0-9]+", "", k)
                # make this change for consistency
                k = "hotspot_b58_addresses" if k == "hotspot_b58_address" else k
                v = []
            elif type(e) is marko.block.Paragraph:
                for c in e.children:
                    if type(c) is marko.inline.RawText and k in ["hotspot_b58_addresses", "hotspot_name"]:
                        v.append(c.children)
        if k and k in ["hotspot_b58_addresses", "hotspot_name"] and v:
            parsed_elem[k] = v

        return parsed_elem
    except IndexError:
        return None


def get_issues(since: Optional[str] = None):
    issues = []
    page = 1
    while True:
        if since:
            url = f"https://api.github.com/repos/helium/denylist/issues?state=all&page={page}&per_page=100&since={since}"
        else:
            url = f"https://api.github.com/repos/helium/denylist/issues?state=all&page={page}&per_page=100"

        payload = {}
        headers = {"accept": "application/vnd.github+json",
                   "Authorization": f"token {os.getenv('GITHUB_ACCESS_TOKEN')}"}

        response = requests.request("GET", url, headers=headers, data=payload).json()
        if len(response) > 0:
            for r in response:
                # submission type: {addition, removal, other}
                labels = [l["name"] for l in r["labels"]]
                if "addition" in labels:
                    issue_type = "addition"
                elif "removal" in labels:
                    issue_type = "removal"
                else:
                    issue_type = "other"
                issues.append({
                    "number": r["number"],
                    "title": r["title"],
                    "user": r["user"]["login"],
                    "labels": labels,
                    "issue_type": issue_type,
                    "state": r["state"],
                    "created_at": r["created_at"],
                    "updated_at": r["updated_at"],
                    "closed_at": r["closed_at"],
                    "comments": r["comments"],
                    "body": r["body"],
                    "reactions": r["reactions"],
                    # "reports_generated": False
                })

        else:
            break
        page += 1
    return issues


def get_entries(issues: list, gateway_inventory: pd.DataFrame):
    p = parser.Parser()
    entries = []
    for issue in issues:
        if issue["body"]:
            parsed = parse_body(p, issue["body"])
            if parsed:
                if "hotspot_b58_addresses" in parsed:
                    for a in parsed["hotspot_b58_addresses"]:
                        if a in gateway_inventory.index:
                            row = gateway_inventory.loc[a]
                            entries.append({
                                "address": a,
                                "issue_number": issue["number"],
                                # "reports_generated": False,
                                # "review_status": "not_reviewed",
                                "name": row["name"],
                                "location": row["location"],
                                "payer": row["payer"],
                                "owner": row["owner"],
                                "maker": row["maker"],
                                "long_country": row["long_country"],
                                "long_state": row["long_state"],
                                "long_city": row["long_city"],
                                "first_block": int(row["first_block"])
                            })
                elif "hotspot_name" in parsed:
                    for a in parsed["hotspot_name"]:
                        name = a.lower().replace(" ", "-")
                        try:
                            a = gateway_inventory.index[gateway_inventory.name == name][0]
                            row = gateway_inventory.loc[a]
                            entries.append({
                                "address": a,
                                "issue_number": issue["number"],
                                # "reports_generated": False,
                                # "review_status": "not_reviewed",
                                "name": row["name"],
                                "location": row["location"],
                                "payer": row["payer"],
                                "owner": row["owner"],
                                "maker": row["maker"],
                                "long_country": row["long_country"],
                                "long_state": row["long_state"],
                                "long_city": row["long_city"],
                                "first_block": int(row["first_block"])
                            })
                        except IndexError:
                            continue
    return entries


def get_pulls():
    """
    Get PR's and parse them for the issues they mention.
    :return: The list of PR's with details and the list of linkages from pull to issue for our join table
    """
    pulls = []
    page = 1
    while True:
        url = f"https://api.github.com/repos/helium/denylist/pulls?state=all&page={page}&per_page=100"

        payload = {}
        headers = {"accept": "application/vnd.github+json",
                   "Authorization": f"token {os.getenv('GITHUB_ACCESS_TOKEN')}"}

        response = requests.request("GET", url, headers=headers, data=payload).json()
        if len(response) > 0:
            for r in response:

                pulls.append({
                    "number": r["number"],
                    "title": r["title"],
                    "user": r["user"]["login"],
                    "state": r["state"],
                    "created_at": r["created_at"],
                    "updated_at": r["updated_at"],
                    "closed_at": r["closed_at"],
                    "body": r["body"],
                })
        else:
            break
        page += 1

    # seeing a couple of different patterns for closures
    p1 = re.compile(r"Closes #(\d+)")
    p2 = re.compile(r"Closes https://github.com/helium.denylist/issues/(\d+)")
    issue_joins = []
    for pull in pulls:
        b = pull["body"]
        if b:
            rows = b.splitlines()
            for row in rows:
                m1 = p1.findall(row)
                m2 = p2.findall(row)
                matches = m1 if len(m1) >= len(m2) else m2
                if len(matches) > 0:
                    issue_joins.append({"pull": pull["number"], "issue": matches[0]})
    return pulls, issue_joins