# Changes in 1.20

## Breaking changes


## New features


## Bug fixes


## Improvements

* Support passing memory value as `str` in `GdsSessions::get_or_create`. Allow passing memory value from `GdsSessions::list`.
* Warn about expiring sessions only if remaining time is < 1h instead of < 1day.


## Other changes

* Support Python 3.14
* Add support for PyArrow 23
* Drop support for PyArrow 17
