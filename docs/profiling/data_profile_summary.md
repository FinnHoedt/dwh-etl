# Data Profiling Summary

This report is generated from local `data_input` files and is intended to support mapping and null/format handling decisions.

## crashes

- Rows: 2244855
- Columns: 29

| Column | Dtype | Null % | Distinct | Numeric Parse % | Date Parse % |
|---|---:|---:|---:|---:|---:|
| crash_date | str | 0.00% | 4987 | 0.00% | 100.00% |
| crash_time | str | 0.00% | 1440 | 0.00% | 100.00% |
| borough | str | 30.53% | 5 | 0.00% | 0.00% |
| zip_code | str | 30.55% | 236 | 100.00% | 0.00% |
| latitude | float64 | 10.72% | 130168 | 100.00% | 0.00% |
| longitude | float64 | 10.72% | 101004 | 100.00% | 0.00% |
| location | str | 10.72% | 390593 | 0.00% | 0.00% |
| on_street_name | str | 21.89% | 23095 | 0.00% | 0.02% |
| cross_street_name | str | 38.26% | 25142 | 0.02% | 0.00% |
| off_street_name | str | 82.23% | 265439 | 0.00% | 0.00% |
| number_of_persons_injured | float64 | 0.00% | 32 | 100.00% | 0.00% |
| number_of_persons_killed | float64 | 0.00% | 7 | 100.00% | 0.00% |
| number_of_pedestrians_injured | int64 | 0.00% | 14 | 100.00% | 0.00% |
| number_of_pedestrians_killed | int64 | 0.00% | 6 | 100.00% | 0.00% |
| number_of_cyclist_injured | int64 | 0.00% | 5 | 100.00% | 0.00% |
| number_of_cyclist_killed | int64 | 0.00% | 3 | 100.00% | 0.00% |
| number_of_motorist_injured | int64 | 0.00% | 31 | 100.00% | 0.00% |
| number_of_motorist_killed | int64 | 0.00% | 6 | 100.00% | 0.00% |
| contributing_factor_vehicle_1 | str | 0.36% | 61 | 0.00% | 0.00% |
| contributing_factor_vehicle_2 | str | 16.18% | 61 | 0.00% | 0.00% |
| contributing_factor_vehicle_3 | str | 92.76% | 54 | 0.00% | 0.00% |
| contributing_factor_vehicle_4 | str | 98.35% | 43 | 0.00% | 0.00% |
| contributing_factor_vehicle_5 | str | 99.55% | 34 | 0.00% | 0.00% |
| collision_id | int64 | 0.00% | 2244855 | 100.00% | 0.00% |
| vehicle_type_code_1 | str | 0.74% | 1869 | 0.00% | 0.00% |
| vehicle_type_code_2 | str | 20.26% | 2080 | 0.00% | 0.00% |
| vehicle_type_code_3 | str | 93.05% | 301 | 0.00% | 0.00% |
| vehicle_type_code_4 | str | 98.41% | 116 | 0.00% | 0.00% |
| vehicle_type_code_5 | str | 99.56% | 78 | 0.00% | 0.00% |

## persons

- Rows: 5901072
- Columns: 21

| Column | Dtype | Null % | Distinct | Numeric Parse % | Date Parse % |
|---|---:|---:|---:|---:|---:|
| unique_id | int64 | 0.00% | 5901072 | 100.00% | 4.38% |
| collision_id | int64 | 0.00% | 1614722 | 100.00% | 0.02% |
| crash_date | str | 0.00% | 4987 | 0.00% | 100.00% |
| crash_time | str | 0.00% | 1440 | 0.00% | 100.00% |
| person_id | str | 0.00% | 5706249 | 2.32% | 0.08% |
| person_type | str | 0.00% | 4 | 0.00% | 0.00% |
| person_injury | str | 0.00% | 3 | 0.00% | 0.00% |
| vehicle_id | float64 | 4.17% | 2739297 | 100.00% | 0.00% |
| person_age | str | 11.24% | 910 | 99.84% | 0.16% |
| ejection | str | 48.58% | 6 | 0.00% | 0.00% |
| emotional_status | str | 46.89% | 8 | 0.00% | 0.00% |
| bodily_injury | str | 46.88% | 14 | 0.00% | 0.00% |
| position_in_vehicle | str | 48.57% | 11 | 0.00% | 0.00% |
| safety_equipment | str | 52.11% | 17 | 0.00% | 0.00% |
| ped_location | str | 98.21% | 4 | 0.00% | 0.00% |
| ped_action | str | 98.21% | 16 | 0.00% | 0.00% |
| complaint | str | 46.88% | 21 | 0.00% | 0.00% |
| ped_role | str | 3.30% | 10 | 0.00% | 0.00% |
| contributing_factor_1 | str | 98.23% | 53 | 0.00% | 0.00% |
| contributing_factor_2 | str | 98.24% | 53 | 0.00% | 0.00% |
| person_sex | str | 11.04% | 3 | 0.00% | 0.00% |

## vehicles

- Rows: 4502923
- Columns: 25

| Column | Dtype | Null % | Distinct | Numeric Parse % | Date Parse % |
|---|---:|---:|---:|---:|---:|
| unique_id | int64 | 0.00% | 4502923 | 100.00% | 4.56% |
| collision_id | int64 | 0.00% | 2242902 | 100.00% | 3.86% |
| crash_date | str | 0.00% | 4987 | 0.00% | 100.00% |
| crash_time | str | 0.00% | 1440 | 0.00% | 100.00% |
| vehicle_id | str | 0.00% | 2974098 | 82.52% | 0.78% |
| state_registration | str | 8.04% | 82 | 0.00% | 0.00% |
| vehicle_type | str | 6.12% | 3101 | 0.00% | 0.00% |
| vehicle_make | str | 43.22% | 14578 | 0.00% | 0.00% |
| vehicle_model | str | 98.86% | 2429 | 0.20% | 0.24% |
| vehicle_year | float64 | 43.78% | 340 | 100.00% | 0.00% |
| travel_direction | str | 37.54% | 15 | 0.00% | 0.00% |
| vehicle_occupants | str | 40.51% | 135 | 100.00% | 0.00% |
| driver_sex | str | 51.80% | 3 | 0.00% | 0.00% |
| driver_license_status | str | 54.13% | 3 | 0.00% | 0.00% |
| driver_license_jurisdiction | str | 54.05% | 72 | 0.00% | 0.00% |
| pre_crash | str | 21.03% | 19 | 0.00% | 0.00% |
| point_of_impact | str | 38.32% | 19 | 0.00% | 0.00% |
| vehicle_damage | str | 38.95% | 19 | 0.00% | 0.00% |
| vehicle_damage_1 | str | 60.31% | 19 | 0.00% | 0.00% |
| vehicle_damage_2 | str | 69.71% | 19 | 0.00% | 0.00% |
| vehicle_damage_3 | str | 76.45% | 19 | 0.00% | 0.00% |
| public_property_damage | str | 33.95% | 3 | 0.00% | 0.00% |
| public_property_damage_type | str | 99.32% | 22102 | 0.00% | 0.00% |
| contributing_factor_1 | str | 3.74% | 61 | 0.02% | 0.00% |
| contributing_factor_2 | str | 38.03% | 56 | 0.00% | 0.00% |
