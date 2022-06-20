package bigdata.transformations

import bigdata.case_classes.Flights

class Transformation(value:Int) {
  val compare_value: Int =value
  def airport_filtering(flight: Flights): Boolean = {
    flight.count > compare_value;
  }
}
