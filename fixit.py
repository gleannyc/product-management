import os
from copy import deepcopy
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
import pandas as pd


def parse_label_issues(label: dict):
    label = deepcopy(label)
    score = float(label["name"].lstrip('fixit-score-'))
    issues = label["issues"]["nodes"]

    if len(issues) == 0:
        return []
    else:
        for issue in issues:
            assignee_name = (
                "unassigned" if issue["assignee"] is None else issue["assignee"]["name"]
            )
            issue.update(
                dict(
                    assignee=assignee_name,
                    fixit_score=score,
                    state=issue["state"]["name"],
                )
            )

        return issues


def get_issues(client) -> list:
    labels_query = gql(
        """
      query Query {
        team(id: "56333a07-ac14-4ba8-b88d-a24b6fae348e") {
          labels(filter: { parent: { name: { eq: "fixit-scores" } } }) {
            nodes {
              name
              issues {
                nodes {
                  id
                  identifier
                  state {
                    name
                  }
                  assignee {
                    name
                  }
                }
              }
            }
          }
        }
      }
      """
    )

    labels = client.execute(labels_query)
    label_data = labels["team"]["labels"]["nodes"]
    results = [parse_label_issues(label) for label in label_data]
    flat_results = sum(results, [])
    return flat_results


def set_priorities(client, issues):
    fixit_to_priority_map = dict(
        [
            (0, 0),
            (0.5, 4),
            (1, 3),
            (2, 2),
            (4, 1),
        ]
    )

    results = []
    for issue in issues:
        priority = fixit_to_priority_map[issue["fixit_score"]]

        query = gql(
            f"""
            mutation IssueUpdate {{
            issueUpdate(
                id: "{issue['id']}",
                input: {{
                priority: {priority}
                }}
            ) {{
                success 
                    issue {{ id }}
            }}
            }}
            """
        )

        result = client.execute(query)
        results.append(result)

    return results


def print_current_totals(issues):
    df = pd.DataFrame.from_records(issues)

    totals = (
        df[df["state"] == "Done"]
        .groupby("assignee", as_index=False)["fixit_score"]
        .sum()
        .rename(columns=dict(fixit_score='fixit_score_total'))
        .sort_values('assignee')
    )

    if totals.empty:
        print("no completed fixit issues yet")
    else:
        print(totals)

    return True


def main():

    token = os.getenv("LINEAR_API_TOKEN")

    if token is None:
        print(
            "\n\tEnvironmental variable LINEAR_API_TOKEN must be set with a Linear API developer token\n\n\tGo to https://linear.app/glean/settings/api to generate a token.\n"
        )
        return

    transport = AIOHTTPTransport(
        url="https://api.linear.app/graphql",
        headers={"Authorization": token},
    )

    client = Client(transport=transport, fetch_schema_from_transport=True)
    issues = get_issues(client)
    _ = set_priorities(client, issues)
    print_current_totals(issues)

    return True


if __name__ == "__main__":
    main()
