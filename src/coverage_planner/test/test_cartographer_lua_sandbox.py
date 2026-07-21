#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pathlib
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
LUA_DICTIONARY_SOURCE = (
    REPO_ROOT
    / "src"
    / "cartographer"
    / "cartographer"
    / "common"
    / "lua_parameter_dictionary.cc"
)


class CartographerLuaSandboxSourceTest(unittest.TestCase):
    def test_unsafe_globals_are_removed_before_configuration_evaluation(self):
        source = LUA_DICTIONARY_SOURCE.read_text(encoding="utf-8")
        function_start = source.index("void DisableUnsafeLuaGlobals(lua_State* L)")
        function_end = source.index("\n}\n", function_start)
        function = source[function_start:function_end]

        for name in (
            "os",
            "io",
            "package",
            "debug",
            "require",
            "dofile",
            "loadfile",
            "load",
            "loadstring",
            "module",
            "collectgarbage",
        ):
            self.assertIn('"%s"' % name, function)
        self.assertIn("lua_pushnil(L)", function)
        self.assertIn("lua_setglobal(L, name)", function)

        open_libraries = source.index("luaL_openlibs(L_)")
        disable_globals = source.index("DisableUnsafeLuaGlobals(L_)", open_libraries)
        load_config = source.index("luaL_loadstring(L_, code.c_str())", disable_globals)
        self.assertLess(open_libraries, disable_globals)
        self.assertLess(disable_globals, load_config)


if __name__ == "__main__":
    unittest.main()
