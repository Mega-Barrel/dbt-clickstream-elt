
""" Clickstream Data Config Class """

class DataConfig:
    """ Configuration for clickstream data generation """
    EVENT_TYPES = {
        "values": [
            "page_view",
            "click",
            "form_submit",
            "add_to_cart",
            "purchase",
        ],
        "probabilities": [
            0.4,
            0.25,
            0.2,
            0.1,
            0.05,
        ]
    }

    PAGES = [
        "home",
        "product",
        "cart",
        "checkout",
        "confirmation",
    ]

    REFERENCES = [
        "Direct",
        "Instagram",
        "Referral",
        "Facebook",
        "Google",
        "Organic"
    ]

    DEVICES = [
        "mobile",
        "desktop",
        "tablet",
    ]

    DEVICE_MODEL = {
        "mobile": [
            'Android',
            'IOS'
        ]
    }

    NUM_USERS = 1000

    DATE_RANGE_SECONDS = 7 * 24 * 3600
