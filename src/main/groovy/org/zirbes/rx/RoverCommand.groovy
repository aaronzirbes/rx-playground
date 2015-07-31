package org.zirbes.rx

class RoverCommand {

    Integer order
    String color
    String who

    String toString() {
        "${order}: ${color} rover, ${color} rover, send ${who} right over."
    }

}

