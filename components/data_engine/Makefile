CXX = g++
CXXFLAGS = -std=c++17 

ABSL_LIBRARIES = $(shell find ../deps/out/lib/libabsl_*.a -printf '%f\n' \
		| sed -e 's/libabsl_\([a-z0-9_]\+\)\.a/-labsl_\1/g')


LDFLAGS = -L../deps/out/lib  \
		-Wl,-Bstatic -ltkrzw \
		-Wl,--start-group $(ABSL_LIBRARIES) -Wl,--end-group \
		-Wl,-Bdynamic -lpthread -ldl \
		-Wl,--gc-sections



SRC_EXT = cpp
BUILD_PATH := ./build
BIN_PATH := ../bin
SRC_PATH = .
INCLUDES = -I$(SRC_PATH) -I../deps/out/include -I$(SRC_PATH)/lib  -Iinclude -I../deps/tkrzw -I../deps/cpp-httplib -I../deps/json/single_include/nlohmann

CXXFLAGS += $(INCLUDES)



# 源文件和目标文件路径
SRCS = $(shell find $(SRC_PATH) -name '*.$(SRC_EXT)')
BIN_SOURCES = $(shell find $(SRC_PATH)/bin -name '*.$(SRC_EXT)')

# 剥离根目录路径，转移到build下相应目录
OBJECTS = $(SRCS:$(SRC_PATH)/%.$(SRC_EXT)=$(BUILD_PATH)/%.o)
BIN_OBJECTS = $(BIN_SOURCES:$(SRC_PATH)/%.$(SRC_EXT)=$(BUILD_PATH)/%.o)
NON_BIN_OBJECTS = $(filter-out $(BIN_OBJECTS),$(OBJECTS))

# build/bin下的目标文件转移到BIN_PATH中
TARGET = $(BIN_OBJECTS:$(BUILD_PATH)/bin/%.o=$(BIN_PATH)/%)

all: dirs
	@echo "Sources: $(SRCS)"
	@echo "Bin Sources: $(BIN_SOURCES)"
	@echo "Objects: $(BIN_OBJECTS)"
	@echo "Bin Objects: $(BIN_OBJECTS)"
	@echo "Targets: $(TARGET)"
	@$(MAKE) binary

binary: $(TARGET)

$(BIN_PATH)/%: $(BUILD_PATH)/bin/%.o $(NON_BIN_OBJECTS)
	@echo "Linking: $@"
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)


$(BUILD_PATH)/%.o: $(SRC_PATH)/%.$(SRC_EXT)
	$(CXX) $(CXXFLAGS) -c $< -o $@

.PHONY: dirs
dirs:
	@mkdir -p $(dir $(OBJECTS))
	@mkdir -p $(BIN_PATH)


clean:
	@$(RM) -r $(BUILD_PATH) $(BIN_PATH)