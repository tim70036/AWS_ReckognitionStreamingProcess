
def SendDrunkMessage():
    import boto3
    import pprint
    p = boto3.client('pinpoint')

    response = p.send_messages(
        ApplicationId='ef70469050f448fe898547f2b9d1c401',
        MessageRequest={

            'Addresses': {
                'ed_V1lar0DY:APA91bGtex90hkeNTiCm9iAhviUNxwvrT9NpRU5aDEvpwE9oPF4DuqpIkIAYELgIDtUGG_7vZskxtvz_yv9vfS_sHqAa7hySN73KdSGUOf_WCULfLG1EipI6BsxZTkeTcf3PKMvjBMO_': {
                    'BodyOverride': 'string',
                    'ChannelType': 'GCM',
                }
            },
            'MessageConfiguration': {
                'GCMMessage': {
                    'Action': 'OPEN_APP',
                    'Body': 'dsfsdf',
         
                    'Data': {
                        'asdasd': 'asdasd'
                    },
      
         
                    'Title': 'fuck',
      
                }
            }
        }
    )

    pprint.pprint(response)