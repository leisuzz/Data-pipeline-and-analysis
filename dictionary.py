import sys
import unicodedata


# construct dataframe
class Df:
    def __init__(self):
        super(Df, self).__init__()

        # construct UML
        self.app = {'server_received_time': [], 'app': [], 'device_carrier': [], '$schema': [],
                    'city': [], 'user_id': [], 'uuid': [], 'event_time': []}

        # user properties extends
        self.user_prop = {}
        self.cbc = {'CBC.userID': [], 'CBC.userTier': []}
        self.exp = {'experiment.id': [], 'experiment.variant': []}
        self.user = {'user.id': [], 'user.id.cbcplus': [], 'user.id.cbcvisitor': [], 'user.tier': []}
        self.location = {'location.news': [], 'location.radio': [], 'location.region': [], 'location.tv': [],
                         'location.weather': []}
        self.referc = {'referrer.campaign': []}

        # platform extends
        self.amp_att_ids = {'element': []}
        self.data = {'first_event': []}
        self.group = {}

        # event_properties
        self.event_properties = {'U.vf': [], 'cmfAppId': [], 'syndicate': [], 'user.tier': [], 'userTier': []}

        # event_properties extends
        self.refer = {'referrer.campaign': [], 'referrer.pillar': [], 'referrer.url': []}
        self.e_app = {'app.name': [], 'app.version': [], 'app.pillar': []}
        self.custom = {'custom.DNT': [], 'custom.cookiesenabled': [], 'custom.engine': []}
        self.feature = {'feature.name': [], 'feature.origin': [], 'feature.position': []}

        # content
        self.content = {'content.subsection1': [], 'content.subsection2': [], 'content.subsection3': [],
                        'content.subsection4': [],
                        'content.area': [], 'content.tier': [], 'content.title': [], 'content.type': [],
                        'content.id': [], 'content.cms': [], 'content.authenticated': [], 'content.authors': [],
                        'content.originaltitle': [], 'content.pubdate': [],
                        'content.publishedtime': [], 'content.updatedtime': [], 'content.url': []}

        # content extends
        self.cont_med = {'content.media.audiovideo': [], 'content.media.duration': [],
                         'content.media.episodenumber': [],
                         'content.media.genre': [], 'content.media.length': [], 'content.media.liveondemand': [],
                         'content.media.region': [], 'content.media.seasonnumber': [], 'content.media.show': [],
                         'content.media.sport': []}
        self.cont_keyw = {'content.keywords.collections': [], 'content.keywords.company': [],
                          'content.keywords.location': [], 'content.keywords.organization': [],
                          'content.keywords.person': [],
                          'content.keywords.subject': [], 'content.keywords.tag': []}

        # platform const
        self.platform = {'platform': [], 'os_version': [], 'amplitude_id': [], 'processed_time': [],
                         'user_creation_time': [], 'version_name': [],
                         'ip_address': [], 'paying': [], 'dma': [],
                         'client_upload_time': [],
                         '$insert_id': [], 'groups': [], 'group_properties': [],
                         'event_type': [], 'library': [], 'device_type': [], 'device_manufacturer': [],
                         'start_version': [], 'location_lng': [],
                         'amplitude_event_type': [], 'server_upload_time': [], 'event_id': [], 'location_lat': [],
                         'os_name': [], 'device_brand': [],
                         'amplitude_attribution_ids': [],
                         'data': []}

        # update dict for user_properties
        self.user_prop.update(self.cbc)
        self.user_prop.update(self.exp)
        self.user_prop.update(self.user)
        self.user_prop.update(self.location)
        self.user_prop.update(self.referc)

        # update dict for content
        self.content.update(self.cont_med)
        self.content.update(self.cont_keyw)

        # update dict for event_properties
        self.event_properties.update(self.refer)
        self.event_properties.update(self.e_app)
        self.event_properties.update(self.feature)
        self.event_properties.update(self.custom)
        self.event_properties.update(self.content)

        # last info from df
        self.event_info = {'device_id': [], 'language': [], 'device_model': [], 'country': [],
                           'region': [],
                           'is_attribution_event': [],
                           'adid': [], 'session_id': [], 'device_family': [], 'sample_rate': [], 'idfa': [],
                           'client_event_time': []}

    def printout(self, x, file_path):

        # obtain info from struct type of user_ and event_ properties
        x_user = x['user_properties'].asDict()
        x_event = x['event_properties'].asDict()

        for key, value in self.app.items():
            # get value so that output print in UML order
            # check whether key exist in input data
            if key in x:
                if x[key] is not None:  # check whether value is empty or not
                    value = x[key]

                    # ignore strings like '\xa0\' and etc.
                    value = unicodedata.normalize("NFKD", str(value))
                else:
                    value = x[key]

                # store value in the dictionary
                self.app[key].append(value)

                # print output in txt file
                sys.stdout = open(file_path, "a")
                print(f'{key}: {value}\n')

        # print outputs from platform
        for key, value in self.platform.items():
            if key in x:
                if x[key] is not None:
                    value = x[key]
                    value = unicodedata.normalize("NFKD", str(value))
                else:
                    value = x[key]
                self.platform[key].append(value)
                sys.stdout = open(file_path, "a")
                print(f'{key}: {value}\n')

        # print outputs from user_properties
        for key, value in self.user_prop.items():
            if key in x_user:
                # get value from user_properties
                if x_user[key] is not None:
                    value = x_user[key]
                    value = unicodedata.normalize("NFKD", str(value))
                else:
                    value = x_user[key]
                self.user_prop[key].append(value)
                sys.stdout = open(file_path, "a")
                print(f'{key}: {value}\n')

        # print outputs from event_properties
        for key, value in self.event_properties.items():
            if key in x_event:
                # get value from event_properties
                if x_event[key] is not None:
                    value = x_event[key]
                    value = unicodedata.normalize("NFKD", str(value))
                else:
                    value = x_event[key]
                self.event_properties[key].append(value)
                sys.stdout = open(file_path, "a")
                print(f'{key}: {value}\n')

        # print outputs from event_info
        for key, value in self.event_info.items():
            if key in x:
                if x[key] is not None:
                    value = x[key]
                    value = unicodedata.normalize("NFKD", str(value))
                else:
                    value = x[key]
                self.event_info[key].append(value)
                sys.stdout = open(file_path, "a")
                print(f'{key}: {value}\n')
        return self.app, self.platform, self.user_prop, self.event_properties, self.event_info
