# VIIRS Nightfire, MODIS C6 and VIIRS 375 m active fire detection data product: Gas Flares and Fire Events at Oil&Gas Facilities Dashboards

This projects aims to produce timeseries for and corelate obsrvations of thermal anomalies obtained using VIIRS Nightfire, MODIS and VIIRS active fire detection algorithms. So far the main object for research were gas flares and fires occuring at oil and gas industry facilities which exhibit higher overall temperatures compared with other open flame sources such as, e.g., vegetation fires.

# Background

**VIIRS Nightfire** (**VNF**) is a remote sensing algorithm developed for VIIRS multispectral sensor which uses all its available IR spectral bands to detect high-temperature thermal anomalies (hotspots) at nighttime when the background noise is minimal in NIR/SWIR-range channels [1]. SWIR fire detection and data (temperature, area) obtained via Plank curve fitting for all available spectral channels allows the algorithm to be more sensitive to higher temperature hotspots, such as gas flares typical for oil and gas industry facilities. The algorithm also presents estimates of flared or burnt gas volumes based on radiant heat, which in turn is calculated from the Stefan–Boltzmann law using temprature and area estimates.

VNF has regular and "pct" datasets. While the first only account for local maxima, the latter also uses all detections near the maxima with radiant heat > N * RH<sub>local maxima</sub>. The data is also a subject to a "bow-tie" effect which results into duplicated detection at the edge of the scan.

**MODIS** and **VIIRS active fire products** (**MAFP** and **VAFP**, respectively) are popular algorithms used to detect high-temperature thermal anomalies, primarily vegetation fires [2, 3]. In this project, they are used as an additional data source for VIIRS Nightfire, with a goal of correlating their observations prioducing a means of detecting flares during datetime, when VNF does not obtain data. Active fire detection algorithms' detections can be obatined via NASA FIRMS system: https://firms.modaps.eosdis.nasa.gov/download/. Both algorithms estimate fire radiative power (FRP) for the hotspot based on experimental values obtained from burning biomass fuels at ~1 000 K.

### Test data

Test dataset contains data for Erginskoye (Russia, routine gas flaring), Anastasievsko-Troitskoye (Russia, fire event, later routine gas flaring) and Bulla-Deniz (Azerbaijan, fire event) fields, as well as three separate flaring facilities at Adgaz LNG (UAE, routine gas flaring) from April, 2012 till September, 2022.

# Usage

The project requires Python 3.7 with ```pandas```, ```matplotlib```, ```numpy```, ```scipy```, ```geopandas``` and ```shapely``` libraries installed.

Active fire detection (**AFP**) algortihms' detection data in csv format should be stored in subfolders corresponding to field name (exact name match is not important). MODIS AFP detections are obtained from any file with "M-C61" in its name; VIIRS AFP detections are obtained from any file with ```SV-C2``` or ```J1V-C2``` in its name.

VIIRS Nightfire detections (**VNF**) storage is identical to AFP: VNF folder → subfolders → csv files. Regular VNF timeseries have ```_vnf_``` in their name; pct timeseries have ```_pct_``` in their name.

The project also requires a **ROI** (region of interest) shapefile in the ROI folder containing boundaries and object names ('field') which is used to clip fire algorithm detections. The 'field' name is later used as timeseries name. Any detection within the ROI object boundaries is attributed to this object; detections outside the boundaries are discarded. 

Using all available CPU resources may result in errors on the later stages of the program run. Reducing CPU threads utilization in calculations may fix this issue. Ctrl + F for ```multiprocessing.Pool(multiprocessing.cpu_count() - 1)``` and replace ```(multiprocessing.cpu_count() - 1)``` with prefered CPU thread count (e.g., ```(4)```).

### Results

The results are stored in the **Results** folder.

1. All unique detections are stored in the root ```Source``` subfolder: MODIS, VIIRS AFP, VNF and VNF_pct. 
1. "Source" tables are then modified and stored in the main ```Results``` folder: MODIS, VIIRS AFP, VNF and VNF_pct. "Bow-tie" effect filtered VNF datasets have a postfix "removed bowtie". For MODIS and VIIRS AFP, pct calculation akin to VNF is applied.
1. Daily sums of the datasets from "1" are aggregated in ```Daily summed datasets``` subfolder per day, as well as per daytime and nighttime only.
1. MODIS and VIIRS AFP datasets are then correlated with VIIRS Nightfire data based on same time observation and daily mean, daytime and nighttime observations, as well as local maxima (MaxRH) only or 75% pct detections (Pct75_RH). The resulting plots are stored in ```Scatters``` subfolder, with all the variables stored in ```fit_paras.csv```.
1. All data for respective object is than displayed as dashboard in ```Dashboards``` subfolder (subfolders are named after 'field' name from ```ROI```). Top plot contains unique VNF temperature estimates, middle plot contains flared/burnt gas volume estimates based on local maxima only (MaxFRP and MaxRH), while the bottom plot uses all detections with FRP or RH > 0.75 from local maxima (Pct75_RH and Pct75_FRP). FRP to MCM (million cubic meters) conversion is based on correltation performed in the previous step (values are stored in ```fit_paras.csv```).
    - Dashboards with ```0``` in their name contain all detections; ```2``` present daily means, and ```3``` is all detections occuring in 2022 only. ```1``` are csv files containg all detection and daily means. Dashed lines correspond to mean values (temperature on the top plot, respective dataset mean volume estimate on the middle and bottom plots).

# References
1. Elvidge, C.D.; Zhizhin, M.; Baugh, K.E.; Hsu, F.-C.; Ghosh, T. Methods for Global Survey of Natural Gas Flaring from Visible Infrared Imaging Radiometer Suite Data. Energies 2016, 9, 1–15. 10.3390/en9010014.
1. Giglio, L.; Schroeder, W.; Justice, C.O. The collection 6 MODIS active fire detection algorithm and fire products. Remote Sens. Environ. 2016, 178, 31–41. 10.1016/j.rse.2016.02.054.
1. Schroeder, W.; Oliva, P.; Giglio, L.; Csiszar, I.A. The New VIIRS 375 m active fire detection data product: Algorithm description and initial assessment. Remote Sens. Environ. 2014, 143, 85–96. 10.1016/j.rse.2013.12.008.
