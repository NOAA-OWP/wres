# Water Resources Evaluation Service (WRES): Python Bindings for Protobuf Evaluation Messages

This resource contains a Python "wheel" artifact with the pre-compiled Protobuf
bindings for Python clients that want to serialize or deserialize evaluation
messages from the WRES.

This resource is provided "as is" and should not be considered canonical.
Rather, the Protobuf schema files are canonical. These schema files are 
distributed separately.

## Installation

To install the Python bindings using the Python Package Installer (pip):

```
pip install wresproto-0.0.1-py3-none-any.whl
```

Also see:

https://packaging.python.org/tutorials/installing-packages/

## Usage

Import the ```wresproto``` resource into your own Python scripts in order to 
serialize and deserialize WRES evaluation messages in Protobuf format. 
For example:

```python
from wresproto import time_scale_pb2 as TimeScale
from google.protobuf import json_format as JsonFormat

scale = TimeScale.TimeScale()
scale.function = TimeScale.TimeScale.TimeScaleFunction.TOTAL
scale.period.seconds = 123456
scale.period.nanos = 78

json_string = JsonFormat.MessageToJson( scale )

print( "Serialized scale as json string:" )
print( json_string )
```

## License

United States Department of Commerce
National Oceanic and Atmospheric Administration
National Weather Service
Office of Water Prediction

Contact: Dr. Trey Flowers, Director, Analysis and Prediction Division
trey.flowers@***REMOVED***

Authors:
Hank Herr, Federal
James D. Brown, Contractor
Jesse Bickel, Contractor
Christopher Tubbs, Contractor
Raymond Chui, Federal
Sanian Gaffar, Contractor
Qing Zhu, Contractor
Alexander Maestre, Contractor

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.