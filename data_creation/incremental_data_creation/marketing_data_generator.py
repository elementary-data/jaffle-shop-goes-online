from .ads_data_generator import generate_ads_data
from .sessions_data_generator import generate_sessions_data


def generate_marketing_data():
    """Generate all marketing data: ads and sessions"""
    print("Generating marketing data...")

    # Generate ads data first (sessions depend on ad_ids)
    print("1. Generating advertising data...")
    generate_ads_data()

    # Generate sessions data (depends on ads and customers)
    print("2. Generating sessions data...")
    generate_sessions_data()

    print("Marketing data generation complete!")


if __name__ == "__main__":
    generate_marketing_data()
