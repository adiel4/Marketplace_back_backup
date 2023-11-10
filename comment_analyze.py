from googleapiclient import discovery
import json
import config as cfg


def comment_analyze(comment: str):
    client = discovery.build(
        "commentanalyzer",
        "v1alpha1",
        developerKey=cfg.google_api_key,
        discoveryServiceUrl="https://commentanalyzer.googleapis.com/$discovery/rest?version=v1alpha1",
        static_discovery=False,
    )

    analyze_request = {
        'comment': {'text': comment},
        'requestedAttributes': {'TOXICITY': {}}
    }

    try:
        response = client.comments().analyze(body=analyze_request).execute()
        return json.dumps(response, indent=2)
    except Exception as err:
        return {'status': -1, 'err_msg': format(err)}


print(comment_analyze('Тестовое сообщение'))
