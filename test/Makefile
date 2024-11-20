CXX = g++
CXXFLAGS = -std=c++17 
LDFLAGS = -L../deps/cpp-httplib -L../deps/json/single_include/nlohmann

SRC_EXT = cpp
BUILD_PATH := ./build
BIN_PATH := ./bin
SRC_PATH = .

INCLUDES = -I../deps/cpp-httplib -I../deps/json/single_include/nlohmann
CXXFLAGS += $(INCLUDES)

# 目标文件路径
SRCS = $(shell find $(SRC_PATH) -name '*.$(SRC_EXT)')

# 剥离根目录路径，转移到build下相应目录
OBJECTS = $(SRCS:$(SRC_PATH)/%.$(SRC_EXT)=$(BUILD_PATH)/%.o)

# build下的目标文件转移到BIN_PATH中
TARGET = $(OBJECTS:$(BUILD_PATH)/%.o=$(BIN_PATH)/%)

all: dirs
	@$(MAKE) binary

binary: $(TARGET)

clean:
	rm -f $(OBJECTS) $(TARGET)

dirs:
	@mkdir -p $(dir $(OBJECTS))
	@mkdir -p $(dir $(TARGET))
	@mkdir -p $(BIN_PATH)

.PHONY: all clean dirs

$(BIN_PATH)/%: $(BUILD_PATH)/%.o $(OBJECTS)
	@echo "Linking: $@"
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

$(BUILD_PATH)/%.o: $(SRC_PATH)/%.$(SRC_EXT)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(TARGET): $(OBJECTS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)
