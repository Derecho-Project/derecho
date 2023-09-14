// SPDX license identifier: MIT; Project 'GetPot'
// MIT License
//
// Copyright (C) 2001-2016 Frank-Rene Schaefer. 
//
// While not directly linked to license requirements, the author would like 
// this software not to be used for military purposes.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//=============================================================================

#ifndef __INCLUDE_GUARD_GETPOT_HPP__
#define __INCLUDE_GUARD_GETPOT_HPP__

#ifndef    GETPOT_SETTING_THROW_EXCEPTION_ON_DEFAULT
#   define GETPOT_SETTING_THROW_EXCEPTION_ON_DEFAULT false
#endif
#ifndef    GETPOT_SETTING_DEFAULT_TRUE_STRING_LIST
#   define GETPOT_SETTING_DEFAULT_TRUE_STRING_LIST   "true True TRUE yes Yes YES"
#endif
#ifndef    GETPOT_SETTING_DEFAULT_FALSE_STRING_LIST
#   define GETPOT_SETTING_DEFAULT_FALSE_STRING_LIST  "false False FALSE no No NO"
#endif

#ifndef    GETPOT_SETTING_NAMESPACE
    // The '#define' line below can be uncommented, if it is desired to run 
    // GetPot in a particular namespace. Or, the definition of the macro
    // might also happen before including this file.

    #define GETPOT_SETTING_NAMESPACE getpot
#endif

/* Strings used by GetPot; Adaptation for Non-English may happen here. */
#define GETPOT_STR_FILE                         "File"
#define GETPOT_STR_FILE_NOT_FOUND(FILE)         std::string("File \"") + FILE + "\" not found."
#define GETPOT_STR_REACHED_END_OF_CONSTRAINT    std::string("Unexpected end of constraint string reached")
#define GETPOT_STR_MISSING_VARIABLE             "missing variable"
#define GETPOT_STR_VARIABLE                     "variable"
#define GETPOT_STR_VALUE                        "for the value (read in configuration file)"
#define GETPOT_STR_WITH_TYPE                    "of type"
#define GETPOT_STR_IN_SECTION                   "in section"
#define GETPOT_STR_CANNOT_CONVERT_TO(X, TYPE)   std::string("Cannot convert '") + X + "' to type " + (TYPE) + "."
#define GETPOT_STR_DOES_NOT_SATISFY_CONSTRAINT  "does not fulfill the constraint"
#define GETPOT_STR_DOES_NOT_CONTAIN_ELEMENT     "does not contain element"
#define GETPOT_STR_TRUE_FALSE_UNDEFINED         "GetPot: no strings defined for meaning of 'true' and 'false'."

extern "C" {
// Leave the 'extern C' to make it 100% sure to work -
// expecially with older distributions of header files.
#ifndef WIN32
    // this is necessary (depending on OS)
#   include <ctype.h>
#endif
#include <stdio.h>
#include <stdarg.h>
#include <assert.h>
#include <string.h>
}
#include <cmath>
#include <string>
#include <vector>
#include <algorithm>

#include <sstream>
#include <fstream>
#include <iostream> // Not every compiler distribution includes <iostream> 
//                  // with <fstream>
#include <stdexcept>
#include <cstddef>

#ifdef    GETPOT_SETTING_NAMESPACE
namespace GETPOT_SETTING_NAMESPACE {
#endif

typedef  std::vector<std::string>  STRING_VECTOR;

class StringOrCharP {
public:
    std::string content;

    /* This class has the only purpose to cut the number of functions 
     * by factor 2 which are needed to take a std::string and a char pointer. */
    StringOrCharP(const char* Value)        
    { if( Value == 0 ) content = std::string(""); else content = std::string(Value); }
    StringOrCharP(const std::string& Value) 
    { content = std::string(Value); }
};


class GetPot {
    //--------
    void __basic_initialization();
public:
    // (*) constructors, destructor, assignment operator -----------------------
    GetPot();
    GetPot(const GetPot&);
    GetPot(const int argc_, char** argv_, const StringOrCharP FieldSeparator=(const char*)0x0);
    GetPot(const StringOrCharP FileName, 
           const StringOrCharP CommentStart=(const char*)0x0, 
           const StringOrCharP CommentEnd=(const char*)0x0,
           const StringOrCharP FieldSeparator=(const char*)0x0);
    ~GetPot();
#ifndef SWIG
    GetPot& operator=(const GetPot&);
#endif

    // (*) Setup ---------------------------------------------------------------
    //     -- define the possible strings for 'true' and 'false'
    //        i.e. for what are the strings for 'boolean'.
    void   set_true_string_list(unsigned N, const char* StringForTrue, ...);
    void   set_false_string_list(unsigned N, const char* StringForFalse, ...);

    //     -- absorbing contents of another GetPot object
    void   absorb(const GetPot& That);
 
    //     -- for ufo detection: recording requested arguments, options etc.
    void   clear_requests();
    void   disable_request_recording() { __request_recording_f = false; }
    void   enable_request_recording()  { __request_recording_f = true; }


    // (*) direct access to command line arguments -----------------------------
#ifndef SWIG
    const std::string     operator[](unsigned Idx) const;
#endif
    unsigned              size() const;

    // (*) flags ---------------------------------------------------------------
    bool                  options_contain(const char* FlagList) const;
    bool                  argument_contains(unsigned Idx, const char* FlagList) const;

    //     -- get argument at cursor++
    template <class T> T      next(T Default);

    //     -- search for option and get argument at cursor++
    //     -- search for one of the given options and get argument that follows it
    template <class T> T      follow(T Default, const char* Option);
    template <class T> T      follow(T Default, unsigned No, const char* Option, ...);
    
    //     -- directly followed arguments
    template <class T> T      direct_follow(T Default, const char* Option);

    std::vector<std::string>  string_tails(const char* StartString);
    std::vector<int>          int_tails(const char* StartString, const int Default = 1);
    std::vector<double>       double_tails(const char* StartString, const double Default = 1.0);


    //     -- lists of nominuses following an option
    STRING_VECTOR             nominus_followers(const char* Option);
    STRING_VECTOR             nominus_followers(unsigned No, ...);

    // (*) nominus arguments ---------------------------------------------------
    STRING_VECTOR             nominus_vector() const;
    unsigned                  nominus_size() const  { return static_cast<unsigned int>(idx_nominus.size()); }
    std::string               next_nominus();

    // (*) Get command line argument number Idx 
    template <class T> T  get(unsigned Idx, T Default) const;

    // (*) variables -----------------------------------------------------------
    //     -- operator() for quick access to variables
    //     -- get() for constraint based access using 'GetPot' constraints
    STRING_VECTOR         get_section_names() const;
    void                  set_prefix(StringOrCharP Prefix) { prefix = Prefix.content; }
    STRING_VECTOR         get_variable_names() const;
    //     -- scalar values
    template <class T> T  operator()(const StringOrCharP VarName, T Default) const;
    template <class T> T  get(const StringOrCharP VarName);
    template <class T> T  get(const StringOrCharP VarName, const char* Constraint);
    template <class T> T  get(const StringOrCharP VarName, const char* Constraint, 
                              T                   Default);
    //     -- vectors (access element via 'Idx')
    unsigned              vector_variable_size(StringOrCharP VarName) const;
    template <class T> T  operator()(const StringOrCharP VarName, unsigned Idx, T Default)  const;
    template <class T> T  get_element(const StringOrCharP VarName, unsigned Idx, 
                                      const char* Constraint);
    template <class T> T  get_element(const StringOrCharP VarName, unsigned Idx, const char* Constraint, 
                                      T                   Default);

    //     -- setting variables
    //                  i) from outside of GetPot (considering prefix etc.)
    //                  ii) from inside, use '__set_variable()' below
    template <class T> void  set_variable(StringOrCharP VarName, T Value, const bool Requested = true);
    template <class T> void  set(StringOrCharP VarName, T& Value);
    template <class T> void  set(StringOrCharP VarName, T& Value, const char* Constraint);
    template <class T, class U> void  set(StringOrCharP VarName, T& Value, const char* Constraint, U Default);

    // (*) cursor oriented functions -------------------------------------------
    bool            search_failed() const            { return search_failed_f; }

    //     -- search for a certain option and set cursor to position
    bool            search(const char* option);
    bool            search(unsigned No, const char* P, ...);

    //     -- enable/disable search for an option in loop
    void            disable_loop() { search_loop_f = false; }
    void            enable_loop()  { search_loop_f = true; }

    //     -- reset cursor to position '1'
    void            reset_cursor();
    void            init_multiple_occurrence();

    // (*) unidentified flying objects -----------------------------------------
    STRING_VECTOR   unidentified_arguments(unsigned Number, const char* Known, ...) const;
    STRING_VECTOR   unidentified_arguments(const STRING_VECTOR& Knowns) const;
    STRING_VECTOR   unidentified_arguments() const;

    STRING_VECTOR   unidentified_options(unsigned Number, const char* Known, ...) const;
    STRING_VECTOR   unidentified_options(const STRING_VECTOR& Knowns) const;
    STRING_VECTOR   unidentified_options() const;

    std::string     unidentified_flags(const char* Known,
                                       int         ArgumentNumber /* =-1 */) const;

    STRING_VECTOR   unidentified_variables(unsigned Number, const char* Known, ...) const;
    STRING_VECTOR   unidentified_variables(const STRING_VECTOR& Knowns) const;
    STRING_VECTOR   unidentified_variables() const;

    STRING_VECTOR   unidentified_sections(unsigned Number, const char* Known, ...) const;
    STRING_VECTOR   unidentified_sections(const STRING_VECTOR& Knowns) const;
    STRING_VECTOR   unidentified_sections() const;

    STRING_VECTOR   unidentified_nominuses(unsigned Number, const char* Known, ...) const;
    STRING_VECTOR   unidentified_nominuses(const STRING_VECTOR& Knowns) const;
    STRING_VECTOR   unidentified_nominuses() const;

    // (*) output --------------------------------------------------------------
#   ifndef SWIG
    int print() const;
#   endif

private:
    // (*) Type Declaration ----------------------------------------------------
    struct variable {
        //-----------
        // Variable to be specified on the command line or in input files.
        // (i.e. of the form var='12 312 341')

        // -- constructors, destructors, assignment operator
        variable();
        variable(const variable&);
        variable(const std::string& Name, const std::string& Value, const std::string& FieldSeparator);
        ~variable();
        variable& operator=(const variable& That);

        void      take(const std::string& Value, const std::string& FieldSeparator);

        // -- get a specific element in the string vector
        //    (return 0 if not present)
        const std::string*  get_element(unsigned Idx, bool ThrowExceptionF=GETPOT_SETTING_THROW_EXCEPTION_ON_DEFAULT) const;

        // -- data memebers
        std::string       name;      // identifier of variable
        STRING_VECTOR     value;     // value of variable stored in vector
        std::string       original;  // value of variable as given on command line
    };

    // (*) member variables --------------------------------------------------------------
    bool                  built;             // false while in constructor, true afterwards
    std::string           filename;          // the configuration file
    STRING_VECTOR         true_string_list;  // strings that mean 'true'
    STRING_VECTOR         false_string_list; // strings that mean 'false'
    std::string           prefix;            // prefix automatically added in queries
    std::string           section;           // (for dollar bracket parsing)
    STRING_VECTOR         section_list;      // list of all parsed sections
    //     -- argument vector
    STRING_VECTOR         argv;            // vector of command line arguments stored as strings
    unsigned              cursor;          // cursor for argv
    bool                  search_loop_f;   // shall search start at beginning after
    //                                     // reaching end of arg array ?
    bool                  search_failed_f; // flag indicating a failed search() operation
    //                                     // (e.g. next() functions react with 'missed')

    //     --  nominus vector
    int                   nominus_cursor;  // cursor for nominus_pointers
    std::vector<unsigned> idx_nominus;     // indecies of 'no minus' arguments

    //     -- variables
    //       (arguments of the form "variable=value")
    std::vector<variable> variables;
    
    //     -- comment delimiters
    std::string           _comment_start;
    std::string           _comment_end;

    //     -- field separator (separating elements of a vector)
    std::string           _field_separator;

    //     -- some functions return a char pointer to a temporarily existing string
    //        this container makes them 'available' until the getpot object is destroyed.
    std::vector<std::vector<char>* >    __internal_string_container;

    //     -- keeping track about arguments that are requested, so that the UFO detection
    //        can be simplified
    STRING_VECTOR   _requested_arguments;
    STRING_VECTOR   _requested_variables;
    STRING_VECTOR   _requested_sections;

    bool            __request_recording_f;   // speed: request recording can be turned off

    //     -- if an argument is requested record it and the 'tag' the section branch to which 
    //        it belongs. Caution: both functions mark the sections as 'tagged'.
    void __record_argument_request(const std::string& Arg);
    void __record_variable_request(const std::string& Arg);

    // (*) helper functions ----------------------------------------------------
    //  build a GetPot instance
    void __build(const std::string& FileName,
                 const std::string& CommentStart, const std::string& CommentEnd,
                 const std::string& FieldSeparator);
    //   split a string according to delimiters and elements are stored in a vector.
    void __split(std::string str, std::vector<std::string>& vect, std::string delimiters = " \n\t");
    //   set variable from inside GetPot (no prefix considered)
    void __set_variable(const std::string& VarName, const std::string& Value);

    //   -- produce three basic data vectors:
    //      - argument vector
    //      - nominus vector
    //      - variable dictionary
    void __parse_argument_vector(const STRING_VECTOR& ARGV);

    //     -- helpers for argument list processing
    //        * search for a variable in 'variables' array
    const variable*       __find_variable(const std::string&, const std::string& TypeName, 
                                          bool ThrowExceptionF=GETPOT_SETTING_THROW_EXCEPTION_ON_DEFAULT) const;
    template <class T> T  __get(const StringOrCharP VarName, const char* Constraint,
                                bool ThrowExceptionF) const;
    template <class T> T  __get(const StringOrCharP VarName, const char* Constraint, T Default, 
                                bool ThrowExceptionF) const;
    template <class T> T  __get_element(const StringOrCharP VarName, unsigned Idx, const char* Constraint, 
                                        bool ThrowExceptionF) const;
    template <class T> T  __get_element(const StringOrCharP VarName, unsigned Idx, const char* Constraint, 
                                        T Default, bool ThrowExceptionF) const;

    //        * support finding directly followed arguments
    const char*           __match_starting_string(const char* StartString);
    //        * support search for flags in a specific argument
    bool                  __check_flags(const std::string& Str, const char* FlagList) const;
    //        * type conversion if possible
    template <class T> T  __convert_to_type(const std::string& String, T Default, 
                                            bool ThrowExceptionF=GETPOT_SETTING_THROW_EXCEPTION_ON_DEFAULT) const;

    //        * provide a const char* to maintained copies of strings.
    const char*        __get_const_char(const std::string& String) const;

    //        * check for GetPot constraints
    bool               __constraint_check(const std::string& Value, 
                                          const char*        ConstraintStr,
                                          bool               ThrowExceptionF) const;
    bool               __constraint_check_OR(const std::string& Value,      const char** iterator) const;
    bool               __constraint_check_AND(const std::string& Value,     const char** iterator) const;
    bool               __constraint_check_PRIMARY(const std::string& Value, const char** iterator) const;
    bool               __constrain_check_EQUAL_STRING(const char* viterator, const char** iterator) const;
    //        * prefix extraction
    const std::string  __get_remaining_string(const std::string& String, 
                                              const std::string& Start) const;
    //        * search for a specific string
    bool               __search_string_vector(const STRING_VECTOR& Vec,
                                              const std::string& Str) const;

    //     -- helpers to parse input file
    //        create an argument vector based on data found in an input file, i.e.:
    //           1) delete comments (in between '_comment_start' '_comment_end')
    //           2) contract assignment expressions, such as
    //                   my-variable   =    '007 J. B.'
    //             into
    //                   my-variable='007 J. B.'
    //           3) interprete sections like '[../my-section]' etc.
    void               __skip_whitespace(std::istream& istr);
    const std::string  __get_next_token(std::istream& istr);
    const std::string  __get_string(std::istream& istr);
    const std::string  __get_until_closing_bracket(std::istream& istr);

    STRING_VECTOR      __read_in_stream(std::istream& istr);
    STRING_VECTOR      __read_in_file(const std::string& FileName);
    std::string        __process_section_label(const std::string&  Section,
                                               STRING_VECTOR&      section_stack);

    //      -- dollar bracket expressions
    std::string               __DBE_expand_string(const std::string str);
    std::string               __DBE_expand(const std::string str);
    const GetPot::variable*   __DBE_get(const std::string str);
    STRING_VECTOR             __DBE_get_expr_list(const std::string str, const unsigned ExpectedNumber);

    std::string     __double2string(const double& Value) const;
    std::string     __int2string(const int& Value) const;
    STRING_VECTOR   __get_section_tree(const std::string& FullPath);
};

#ifdef GETPOT_SETTING_NAMESPACE
} // namespace GetPotNamespace.
#endif


#endif // __INCLUDE_GUARD_GETPOT_HPP__



