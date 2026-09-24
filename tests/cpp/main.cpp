// CppUnit runner for the C++ client library's unit tests.
#include <cppunit/CompilerOutputter.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

#include <iostream>

int main() {
    CppUnit::TextUi::TestRunner runner;
    runner.addTest(CppUnit::TestFactoryRegistry::getRegistry().makeTest());
    runner.setOutputter(new CppUnit::CompilerOutputter(&runner.result(), std::cerr));
    return runner.run() ? 0 : 1;
}
