import pandas as pd
import locgov_data as ld

search_results = ld.express_search(
    "https://www.loc.gov/search/?dates=1920/1920&fa=location:massachusetts&q=jamaica+plains"
)
print(len(search_results))