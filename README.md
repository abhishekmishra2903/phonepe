# phonepe
streamlit application from phonepe pulse data.

Phonepe has provided some exciting set of data at https://github.com/PhonePe/pulse#readme. Some of the key data points available for each quarter since 2018 are as follows:
1) Transaction count at state and district level
2) Transaction amount at state and district level
3) Registered users at state and district level
4) Total app opens at state and district level
5) Data of Transaction count and Transaction amount are available across 5 different categories, as:
  1) Recharge and bill payments
  2) Peer to peer payments
  3) Merchant payments
  4) Financial services
  5) Others

I have added another data-set pertaining to population distribution so that we can have better visualisation of phonepe penetration with respect to the population of the state. The json file for population has been included here

A geojson file has been used to display some data in map format. The concerned file has been included here as well.

This project is about creating an interactive dashboard to visualise the data and draw some key insights. It shows data at both National and State level and for some interesting plots it also shows the map-view. Complete project has been developed in python environment with the help of pandas, streamlit and plotly libraries.

Note: there is a button on the interface to update the data over SQL. That button need not be pressed unless we have some update in the data. The data is being updated every quarter in the git hub repo, so we would require to use the button accordingly.

