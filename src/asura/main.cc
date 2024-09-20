#include <format>
#include <iostream>

int main() {
    std::cout << std::format("{} + {} = {}", 1, 2, 3) << std::endl;
    return 0;
}