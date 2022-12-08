import re
import copy
import decimal
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport

# TODO add check for one value each of impact, confidence, effort
# if incomplete or multiple, set to triage


def check_for_feedback_label(issue: dict):
    label_names = [node["name"] for node in issue["labels"]["nodes"]]
    return "feedback" in label_names


def custom_round(i: float):
    return int(
        decimal.Decimal(i).quantize(
            decimal.Decimal("1"), rounding=decimal.ROUND_HALF_UP
        )
    )


def prioritize_issues(issues: list):
    issues = copy.deepcopy(issues)
    weights = [issue["weight"] for issue in issues]
    max_weight = max(weights)
    min_weight = min(weights)
    _ = [
        issue.update(
            dict(
                weight_normed=(issue["weight"] - min_weight) / (max_weight - min_weight)
            )
        )
        for issue in issues
    ]
    _ = [
        issue.update(dict(priority_int=4 - custom_round(issue["weight_normed"] * 2)))
        for issue in issues
    ]
    return issues


def parse_issue_rice(issue: dict):
    parsed_issue = dict(id=issue["id"])
    regex = re.compile("[a-z]*-[1-3]")
    labels = [
        node["name"] for node in issue["labels"]["nodes"] if regex.match(node["name"])
    ]
    rice_properties = dict(
        [(label.split("-")[0], int(label.split("-")[1])) for label in labels]
    )

    parsed_issue.update(rice_properties)

    parsed_issue["weight"] = (
        rice_properties["confidence"] * 0.75
        + rice_properties["impact"] * 1.0
        - rice_properties["effort"] * 0.5
    )

    return parsed_issue


def main():

    token = os.getenv("LINEAR_API_TOKEN")

    if token is None:
        print(
            "\n\tEnvironmental variable LINEAR_API_TOKEN must be set with a Linear API developer token\n\n\tGo to https://linear.app/glean/settings/api to generate a token.\n"
        )
        return

    base = "https://api.linear.app/graphql"
    product_team_id = "402d0370-296a-4cfe-86d8-c1cf60dc420d"

    transport = AIOHTTPTransport(
        url=base,
        headers={"Authorization": token},
    )

    client = Client(transport=transport, fetch_schema_from_transport=True)

    query = gql(
        f"""
            query {{
                team(id: "{product_team_id}") {{
                issues {{
                nodes {{
                    id,priority,labels {{ nodes {{name}} }}
                }}
                }}
            }}
            }}
        """
    )

    result = client.execute(query)
    issues = result["team"]["issues"]["nodes"]
    nonpriority_issues = list(filter(lambda issue: issue["priority"] != 1, issues))
    feedback_issues = list(filter(check_for_feedback_label, nonpriority_issues))
    parsed_issues = [parse_issue_rice(issue) for issue in feedback_issues]
    prioritized_issues = prioritize_issues(parsed_issues)

    for issue in prioritized_issues:

        query = gql(
            f"""
            mutation IssueUpdate {{
            issueUpdate(
                id: "{issue['id']}",
                input: {{
                priority: {issue['priority_int']}
                }}
            ) {{
                success 
                    issue {{ id }}
            }}
            }}
            """
        )

        client.execute(query)

    return True


if __name__ == "__main__":
    main()
    print("done")
