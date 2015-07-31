package org.zirbes.rx

import groovy.transform.CompileStatic

@CompileStatic
class Runner {

    static void main(String[] argv) {
        new SqsObservableFactory().observe()
    }
}
