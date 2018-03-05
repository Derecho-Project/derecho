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

#ifndef __INCLUDE_GUARD_GETPOT_CPP__
#define __INCLUDE_GUARD_GETPOT_CPP__

#if defined(WIN32) || defined(SOLARIS_RAW) || (__GNUC__ == 2) || defined(__HP_aCC)
    // WINDOWS or SOLARIS or gcc 2.* or HP aCC
#   define GETPOT_STRTOK(a, b, c)  strtok_r(a, b, c)
#   define GETPOT_STRTOK_3_ARGS
#else
#   define GETPOT_STRTOK(a, b, c)  strtok(a, b)
#endif 

#if defined(WIN32)
#   define GETPOT_SNPRINTF(STR, SIZE, FORMAT_STR, ...) \
           _snprintf(tmp, (size_t)SIZE, FORMAT_STR, __VA_ARGS__) 
#else
#   define GETPOT_SNPRINTF(STR, SIZE, FORMAT_STR, ...) \
           snprintf(tmp, (int)SIZE, FORMAT_STR, __VA_ARGS__) 
#endif

/* In case that a single header is intended, __GETPOT_INLINE is set to 'inline'
 * otherwise, it is set to 'empty', so that normal object code is produced.     */
#ifndef    __GETPOT_INLINE
#   define __GETPOT_INLINE  /*empty*/
#endif

/* The default values for strings that represent 'true' and 'false'             */

#include "GetPot.hpp"

#ifdef    GETPOT_SETTING_NAMESPACE
namespace GETPOT_SETTING_NAMESPACE {
#endif

#define victorate(TYPE, VARIABLE, ITERATOR)                             \
  for(std::vector<TYPE >::const_iterator ITERATOR = (VARIABLE).begin(); \
      (ITERATOR) != (VARIABLE).end(); ++(ITERATOR))

template <class T> class TypeInfo;

template <> class TypeInfo<bool> { 
public: static const char*  name()           { return "bool"; }    
        static bool         default_value()  { return false; }    
        typedef bool        type; 
        static bool         convert(bool& X) { return (bool)X; }
};

template <> class TypeInfo<const char*> { 
public: static const char*   name()                 { return "string"; }  
        static const char *  default_value()        { return ""; }    
        typedef const char*  type; 
        static const char*   convert(const char* X) { return (const char*)X; }
};

template <> class TypeInfo<int> { 
public: static const char*  name()          { return "integer"; } 
        static int          default_value() { return 0; }    
        typedef int         type; 
        static int          convert(int& X) { return (int)X; }
};
template <> class TypeInfo<std::string> : public TypeInfo<const char*> 
{ public: 
    static const char*  convert(std::string& X) { return (const char*)"inadmissible"; } 
    static std::string  default_value()         { return std::string(""); }    
};
template <>   class TypeInfo<unsigned int>      : public TypeInfo<int> 
{ public: static int convert(unsigned int& X)   { return (int)X; } };
template <>   class TypeInfo<char>              : public TypeInfo<int>
{ public: static int convert(char& X)           { return (int)X; } };
template <>   class TypeInfo<unsigned char>     : public TypeInfo<int>
{ public: static int convert(unsigned char& X)  { return (int)X; } };
template <>   class TypeInfo<short>             : public TypeInfo<int> 
{ public: static int convert(short& X)          { return (int)X; } };
template <>   class TypeInfo<unsigned short>    : public TypeInfo<int> 
{ public: static int convert(unsigned short& X) { return (int)X; } };
template <>   class TypeInfo<long>              : public TypeInfo<int> 
{ public: static int convert(long& X)           { return (int)X; } };
template <>   class TypeInfo<unsigned long>     : public TypeInfo<int> 
{ public: static int convert(unsigned long& X)  { return (int)X; } };

template <> class TypeInfo<double> { 
public: static const char*  name()             { return "double"; }  
        static double       default_value()    { return 0.; }    
        typedef double      type; 
        static double       convert(double& X) { return (double)X; }
};
template <> class TypeInfo<float> : public TypeInfo<double> 
{ public: static double convert(float& X) { return (double)X; } };


///////////////////////////////////////////////////////////////////////////////
// (*) constructors, destructor, assignment operator
//.............................................................................
//
__GETPOT_INLINE void
GetPot::__basic_initialization()
{
    cursor = 0;              nominus_cursor = -1;
    search_failed_f = true;  search_loop_f = true;
    prefix = "";             section = "";
    
    // automatic request recording for later ufo detection
    __request_recording_f = true;

    // comment start and end strings
    _comment_start = std::string("#");
    _comment_end   = std::string("\n");

    // default: separate vector elements by whitespaces
    _field_separator = " \t\n";

    // true and false strings
    __split(GETPOT_SETTING_DEFAULT_TRUE_STRING_LIST,  true_string_list);
    __split(GETPOT_SETTING_DEFAULT_FALSE_STRING_LIST, false_string_list);
}

__GETPOT_INLINE
GetPot::GetPot():
  built(false) 
{ 
    __basic_initialization(); 

    STRING_VECTOR _apriori_argv;
    _apriori_argv.push_back(std::string("Empty"));
    __parse_argument_vector(_apriori_argv);
    built = true;
}

__GETPOT_INLINE
GetPot::GetPot(const int argc_, char ** argv_, 
               const StringOrCharP FieldSeparator /* =0x0 */):
  built(false)
    // leave 'char**' non-const to honor less capable compilers ... 
{
    // TODO: Ponder over the problem when the argument list is of size = 0.
    //       This is 'sabotage', but it can still occur if the user specifies
    //       it himself.
    assert(argc_ >= 1);
    __basic_initialization();

    // if specified -> overwrite default string
    if( FieldSeparator.content.length() != 0 ) _field_separator = FieldSeparator.content;

    // -- make an internal copy of the argument list:
    STRING_VECTOR _apriori_argv;
    // -- for the sake of clarity: we do want to include the first argument in the argument vector !
    //    it will not be a nominus argument, though. This gives us a minimun vector size of one
    //    which facilitates error checking in many functions. Also the user will be able to
    //    retrieve the name of his application by "get[0]"
    _apriori_argv.push_back(std::string(argv_[0]));
    int i=1;
    for(; i<argc_; ++i) {
        std::string tmp(argv_[i]);   // recall the problem with temporaries,
        _apriori_argv.push_back(tmp);       // reference counting in arguement lists ...
    }
    __parse_argument_vector(_apriori_argv);
    built = true;
}

__GETPOT_INLINE
GetPot::GetPot(StringOrCharP FileName,
               const StringOrCharP CommentStart  /* = 0x0 */, 
               const StringOrCharP CommentEnd    /* = 0x0 */,
               const StringOrCharP FieldSeparator/* = 0x0 */):
  built(false)
{
    __build(FileName.content, CommentStart.content, CommentEnd.content, FieldSeparator.content);
    built = true;
}

__GETPOT_INLINE
GetPot::GetPot(const GetPot& That)
{ GetPot::operator=(That); }

__GETPOT_INLINE
GetPot::~GetPot()
{ 
    // may be some return strings had to be created, delete now !
    victorate(std::vector<char>*, __internal_string_container, it) {
        delete *it;  
    }
}

__GETPOT_INLINE GetPot&
GetPot::operator=(const GetPot& That)
{
    if (&That == this) return *this;

    built            = That.built;
    filename         = That.filename;
    true_string_list = That.true_string_list;
    false_string_list= That.false_string_list;

    _comment_start  = That._comment_start;
    _comment_end    = That._comment_end;
    _field_separator= That. _field_separator;
    argv            = That.argv;
    variables       = That.variables;
    prefix          = That.prefix;

    cursor          = That.cursor;
    nominus_cursor  = That.nominus_cursor;
    search_failed_f = That.search_failed_f;

    idx_nominus     = That.idx_nominus;
    search_loop_f   = That.search_loop_f;

    return *this;
}


__GETPOT_INLINE void
GetPot::absorb(const GetPot& That)
{
    if (&That == this) return;

    STRING_VECTOR  __tmp(That.argv);

    __tmp.erase(__tmp.begin());

    __parse_argument_vector(__tmp);
}

__GETPOT_INLINE void    
GetPot::clear_requests()
{
    _requested_arguments.erase(_requested_arguments.begin(), _requested_arguments.end());
    _requested_variables.erase(_requested_variables.begin(), _requested_variables.end());
    _requested_sections.erase(_requested_sections.begin(), _requested_sections.end());
}

__GETPOT_INLINE void
GetPot::__parse_argument_vector(const STRING_VECTOR& ARGV)
{
    if( ARGV.size() == 0 ) return;

    // build internal databases:
    //   1) array with no-minus arguments (usually used as filenames)
    //   2) variable assignments:
    //             'variable name' '=' number | string
    STRING_VECTOR                 section_stack;
    STRING_VECTOR::const_iterator it = ARGV.begin();


    section = "";

    // -- do not parse the first argument, so that it is not interpreted a s a nominus or so.
    argv.push_back(*it);
    ++it;

    // -- loop over remaining arguments
    unsigned i=1;
    for(; it != ARGV.end(); ++it, ++i) {
        std::string arg = *it;

        if( arg.length() == 0 ) continue;

        // -- [section] labels
        if( arg.length() > 1 && arg[0] == '[' && arg[arg.length()-1] == ']' ) {

            // (*) sections are considered 'requested arguments'
            if( __request_recording_f ) _requested_arguments.push_back(arg);
            
            const std::string Name = __DBE_expand_string(arg.substr(1, arg.length()-2));
            section = __process_section_label(Name, section_stack);
            // new section --> append to list of sections
            if( find(section_list.begin(), section_list.end(), section) == section_list.end() )
                if( section.length() != 0 ) section_list.push_back(section);
            argv.push_back(arg);
        }
        else {
            arg = section + __DBE_expand_string(arg);
            argv.push_back(arg);
        }

        // -- separate array for nominus arguments
        if( arg[0] != '-' ) idx_nominus.push_back(unsigned(i));

        // -- variables: does arg contain a '=' operator ?
        ptrdiff_t   i  = 0;
        for(std::string::iterator it = arg.begin(); it != arg.end(); ++it, ++i) {
            if( *it != '=' ) continue;

            // (*) record for later ufo detection
            //     arguments carriying variables are always treated as 'requested' arguments. 
            //     as a whole! That is 'x=4712' is  considered a requested argument.
            //
            //     unrequested variables have to be detected with the ufo-variable
            //     detection routine.
            if( __request_recording_f ) _requested_arguments.push_back(arg);

            const bool tmp = __request_recording_f;
            __request_recording_f = false;
            __set_variable(arg.substr(0, i), arg.substr(i+1));  
            __request_recording_f = tmp;
            break;
        }
    }
}


__GETPOT_INLINE STRING_VECTOR
GetPot::__read_in_file(const std::string& FileName)
{
    std::ifstream  i(FileName.c_str());
    if( ! i ) {
        throw std::runtime_error(GETPOT_STR_FILE_NOT_FOUND(FileName));
        return STRING_VECTOR();
    }
    // argv[0] == the filename of the file that was read in
    return __read_in_stream(i);
}

__GETPOT_INLINE STRING_VECTOR
GetPot::__read_in_stream(std::istream& istr)
{
    STRING_VECTOR  brute_tokens;
    while(istr) {
        __skip_whitespace(istr);
        const std::string Token = __get_next_token(istr);
        if( Token.length() == 0 || Token[0] == EOF) break;
        brute_tokens.push_back(Token);
    }

    // -- reduce expressions of token1'='token2 to a single
    //    string 'token1=token2'
    // -- copy everything into 'argv'
    // -- arguments preceded by something like '[' name ']' (section)
    //    produce a second copy of each argument with a prefix '[name]argument'
    unsigned i1 = 0;
    unsigned i2 = 1;
    unsigned i3 = 2;

    STRING_VECTOR  arglist;
    while( i1 < brute_tokens.size() ) {
        const std::string& SRef = brute_tokens[i1];
        // 1) concatinate 'abcdef' '=' 'efgasdef' to 'abcdef=efgasdef'
        // note: java.lang.String: substring(a,b) = from a to b-1
        //        C++ string:      substr(a,b)    = from a to a + b
        if( i2 < brute_tokens.size() && brute_tokens[i2] == "=" ) {
            if( i3 >= brute_tokens.size() )
                arglist.push_back(brute_tokens[i1] + brute_tokens[i2]);
            else
                arglist.push_back(brute_tokens[i1] + brute_tokens[i2] + brute_tokens[i3]);
            i1 = i3+1; i2 = i3+2; i3 = i3+3;
            continue;
        }
        else {
            arglist.push_back(SRef);
            i1=i2; i2=i3; i3++;
        }
    }
    return arglist;
}

__GETPOT_INLINE void
GetPot::__skip_whitespace(std::istream& istr)
    // find next non-whitespace while deleting comments
{
    int tmp = istr.get();
    do {
        // -- search a non whitespace
        while( isspace(tmp) ) {
            tmp = istr.get();
            if( ! istr ) return;
        }

        // -- look if characters match the comment starter string
        unsigned    i=0;
        for(; i<_comment_start.length() ; ++i) {
            if( tmp != _comment_start[i] ) { 
                // NOTE: Due to a 'strange behavior' in Microsoft's streaming lib we do
                // a series of unget()s instead a quick seek. See 
                // http://sourceforge.net/tracker/index.php?func=detail&aid=1545239&group_id=31994&atid=403915
                // for a detailed discussion.

                // -- one step more backwards, since 'tmp' already at non-whitespace
                do istr.unget(); while( i-- != 0 ); 
                return; 
            }
            tmp = istr.get();
            if( ! istr ) { istr.unget(); return; }
        }
        // 'tmp' contains last character of _comment_starter

        // -- comment starter found -> search for comment ender
        unsigned match_no=0;
        while(1+1 == 2) {
            tmp = istr.get();
            if( ! istr ) { istr.unget(); return; }

            if( tmp == _comment_end[match_no] ) { 
                match_no++;
                if( match_no == _comment_end.length() ) {
                    istr.unget();
                    break; // shuffle more whitespace, end of comment found
                }
            }
            else
                match_no = 0;
        }

        tmp = istr.get();

    } while( istr );
    istr.unget();
}

__GETPOT_INLINE const std::string
GetPot::__get_next_token(std::istream& istr)
    // get next concatinates string token. consider quotes that embrace
    // whitespaces
{
    std::string token;
    int    tmp = 0;
    int    last_letter = 0;
    while(1+1 == 2) {
        last_letter = tmp; tmp = istr.get();
        if( tmp == EOF
            || ((tmp == ' ' || tmp == '\t' || tmp == '\n') && last_letter != '\\') ) {
            return token;
        }
        else if( tmp == '\'' && last_letter != '\\' ) {
            // QUOTES: un-backslashed quotes => it's a string
            token += __get_string(istr);
            continue;
        }
        else if( tmp == '{' && last_letter == '$') {
            token += '{' + __get_until_closing_bracket(istr);
            continue;
        }
        else if( tmp == '$' && last_letter == '\\') {
            token += tmp; tmp = 0;  //  so that last_letter will become = 0, not '$';
            continue;
        }
        else if( tmp == '\\' && last_letter != '\\')
            continue;              // don't append un-backslashed backslashes
        token += tmp;
    }
}

__GETPOT_INLINE const std::string
GetPot::__get_string(std::istream& istr)
    // parse input until next matching '
{
    std::string str;
    int    tmp = 0;
    int    last_letter = 0;
    while(1 + 1 == 2) {
        last_letter = tmp; tmp = istr.get();
        if( tmp == EOF)  return str;
        // un-backslashed quotes => it's the end of the string
        else if( tmp == '\'' && last_letter != '\\')  return str;
        else if( tmp == '\\' && last_letter != '\\')  continue; // don't append

        str += tmp;
    }
}

__GETPOT_INLINE const std::string
GetPot::__get_until_closing_bracket(std::istream& istr)
    // parse input until next matching }
{
    std::string str = "";
    int    tmp = 0;
    int    last_letter = 0;
    int    brackets = 1;
    while(1 + 1 == 2) {
        last_letter = tmp; tmp = istr.get();
        if( tmp == EOF) return str;
        else if( tmp == '{' && last_letter == '$') brackets += 1;
        else if( tmp == '}') {
            brackets -= 1;
            // un-backslashed brackets => it's the end of the string
            if( brackets == 0) return str + '}';
            else if( tmp == '\\' && last_letter != '\\')
                continue;  // do not append an unbackslashed backslash
        }
        str += tmp;
    }
}

__GETPOT_INLINE std::string
GetPot::__process_section_label(const std::string&  Section, 
                                STRING_VECTOR&      section_stack)
{
    std::string sname = Section;
    //  1) subsection of actual section ('./' prefix)
    if( sname.length() >= 2 && sname.substr(0, 2) == "./" ) {
        sname = sname.substr(2);
    }
    //  2) subsection of parent section ('../' prefix)
    else if( sname.length() >= 3 && sname.substr(0, 3) == "../" ) {
        do {
            if( section_stack.end() != section_stack.begin() )
                section_stack.pop_back();
            sname = sname.substr(3);
        } while( sname.substr(0, 3) == "../" );
    }
    // 3) subsection of the root-section
    else {
        section_stack.erase(section_stack.begin(), section_stack.end());
        // [] => back to root section
    }

    if( sname != "" ) {
        // parse section name for 'slashes'
        size_t       i            = 0;
        size_t       prev_slash_i = -1;  // points to last slash
        const size_t L            = sname.length();

        for(i = 0; i < L; ++i) {
            if( sname[i] != '/' ) continue;
            if( i - prev_slash_i > 1 ) {
                const size_t Delta = i - (prev_slash_i + 1);
                section_stack.push_back(sname.substr(prev_slash_i + 1, Delta));
            }
            
            prev_slash_i = i;
        }
        if( i - prev_slash_i > 1 ) {
            const size_t Delta = i - (prev_slash_i + 1);
            section_stack.push_back(sname.substr(prev_slash_i + 1, Delta));
        }
    }
    std::string section = "";
    if( section_stack.size() != 0 ) {
        victorate(std::string, section_stack, it) {
            section += *it + "/";
        }
    }
    return section;
}

// convert string to BOOL, if not possible return Default
template <> __GETPOT_INLINE bool
GetPot::__convert_to_type<bool>(const std::string& String, bool Default, 
                                bool ThrowExceptionF /* = GETPOT_SETTING_THROW_EXCEPTION_ON_DEFAULT */) const
{
    // No 'lower' or 'upper' unification. If someone wants that, he 
    // han specify the true-string or fals-strings as he likes.

    victorate(std::string, true_string_list, it1) {
        if( String == *it1 ) return true; 
    }

    victorate(std::string, false_string_list, it2) {
        if( String == *it2 ) return false; 
    }

    if( ThrowExceptionF ) {
        throw std::runtime_error(std::string(GETPOT_STR_FILE) + " \"" + filename + "\": " +
                                 GETPOT_STR_CANNOT_CONVERT_TO(String, TypeInfo<bool>::name()));
    }
    else
        return Default;
}

// convert string to DOUBLE, if not possible return Default
template <> __GETPOT_INLINE double
GetPot::__convert_to_type<double>(const std::string& String, double Default,
                                  bool ThrowExceptionF /* = GETPOT_SETTING_THROW_EXCEPTION_ON_DEFAULT */) const
{
    double tmp;

    if( sscanf(String.c_str(),"%lf", &tmp) == 1 ) return tmp;

    if( ThrowExceptionF )
        throw std::runtime_error(std::string(GETPOT_STR_FILE) + " \"" + filename + "\": " +
                                  GETPOT_STR_CANNOT_CONVERT_TO(String, TypeInfo<double>::name()));
    else
        return Default;
}

// convert string to INT, if not possible return Default
template <> __GETPOT_INLINE int
GetPot::__convert_to_type<int>(const std::string& String, int Default,
                               bool ThrowExceptionF /* = GETPOT_SETTING_THROW_EXCEPTION_ON_DEFAULT */) const
{
    // NOTE: intermediate results may be floating points, so that the string
    //       may look like 2.0e1 (i.e. float format) => use float conversion
    //       in any case.
    return (int)__convert_to_type(String, (double)Default, ThrowExceptionF);
}

// convert string to string, so that GetPot::read method can be generic (template).
template <> __GETPOT_INLINE const char*
GetPot::__convert_to_type<const char*>(const std::string& String, const char* Default,
                                       bool ThrowExceptionF /* = GETPOT_SETTING_THROW_EXCEPTION_ON_DEFAULT */) const
{
    return __get_const_char(String);
}

__GETPOT_INLINE const char*
GetPot::__get_const_char(const std::string& String) const
{
    std::vector<char>* c_str_copy = new std::vector<char>(String.length() + 1, '\0');
    std::copy(String.begin(), String.end(), c_str_copy->begin());

    ((GetPot*)this)->__internal_string_container.push_back(c_str_copy);
    return &(*c_str_copy)[0]; 
}

//////////////////////////////////////////////////////////////////////////////
// (*) cursor oriented functions
//.............................................................................
__GETPOT_INLINE const std::string
GetPot::__get_remaining_string(const std::string& String, const std::string& Start) const
    // Checks if 'String' begins with 'Start' and returns the remaining String.
    // Returns None if String does not begin with Start.
{
    if( Start == "" ) return String;
    // note: java.lang.String: substring(a,b) = from a to b-1
    //        C++ string:      substr(a,b)    = from a to a + b
    if( String.find(Start) == 0 ) return String.substr(Start.length());
    else                          return "";
}

//     -- search for a certain argument and set cursor to position
__GETPOT_INLINE bool
GetPot::search(const char* Option)
{    
    unsigned     OldCursor  = cursor;
    std::string  search_term;
   
    if( ! Option ) return false;
   
    search_term = prefix + Option;

    // (*) record requested arguments for later ufo detection
    __record_argument_request(search_term);                             

    if( OldCursor >= argv.size() ) OldCursor = static_cast<unsigned int>(argv.size()) - 1;
    search_failed_f = true;

    // (*) first loop from cursor position until end
    unsigned  c = cursor;
    for(; c < argv.size(); c++) {
        if( argv[c] == search_term )
        { cursor = c; search_failed_f = false; return true; }
    }
    if( ! search_loop_f ) return false;

    // (*) second loop from 0 to old cursor position
    for(c = 1; c < OldCursor; c++) {
        if( argv[c] == search_term )
        { cursor = c; search_failed_f = false; return true; }
    }
    // in case nothing is found the cursor stays where it was
    return false;
}


__GETPOT_INLINE bool
GetPot::search(unsigned No, const char* P, ...)
{
    // (*) recording the requested arguments happens in subroutine 'search'
    if( No == 0 ) return false;

    // search for the first argument
    if( search(P) == true ) return true;

    // start interpreting variable argument list
    va_list ap;
    va_start(ap, P);
    unsigned i = 1;
    for(; i < No; ++i) {
        char* Opt = va_arg(ap, char *);
        if( search(Opt) == true ) break;
    }
    
    if( i < No ) {
        ++i;
        // loop was left before end of array --> hit but 
        // make sure that the rest of the search terms is marked
        // as requested.
        for(; i < No; ++i) {
            char* Opt = va_arg(ap, char *);
            // (*) record requested arguments for later ufo detection
            __record_argument_request(Opt);
        }
        va_end(ap);
        return true;
    }

    va_end(ap);
    // loop was left normally --> no hit
    return false;
}

__GETPOT_INLINE void
GetPot::reset_cursor()
{ search_failed_f = false; cursor = 0; }

__GETPOT_INLINE void
GetPot::init_multiple_occurrence()
{ disable_loop(); reset_cursor(); }

__GETPOT_INLINE void
GetPot::set_true_string_list(unsigned N, const char* StringForTrue, ...)
{
    true_string_list.clear();

    if( N == 0 ) return;

    va_list   ap;
    unsigned  i = 1;

    va_start(ap, StringForTrue);
    for(; i < N ; ++i) {
        char* Opt = va_arg(ap, char *);
        true_string_list.push_back(std::string(Opt));
    }
    va_end(ap);
}

__GETPOT_INLINE void
GetPot::set_false_string_list(unsigned N, const char* StringForFalse, ...)
{
    false_string_list.clear();

    if( N == 0 ) return;

    va_list   ap;
    unsigned  i = 1;

    va_start(ap, StringForFalse);
    for(; i < N ; ++i) {
        char* Opt = va_arg(ap, char *);
        false_string_list.push_back(std::string(Opt));
    }
    va_end(ap);
}

///////////////////////////////////////////////////////////////////////////////
// (*) direct access to command line arguments
//
__GETPOT_INLINE const std::string
GetPot::operator[](unsigned idx) const
{ return idx < argv.size() ? argv[idx] : ""; }

template <class T> __GETPOT_INLINE T
GetPot::get(unsigned Idx, T Default) const
{
    if( Idx >= argv.size() ) return Default;
    return __convert_to_type(argv[Idx], (typename TypeInfo<T>::type)Default);
}

template <> __GETPOT_INLINE const char*
GetPot::get<const char*>(unsigned Idx, const char* Default) const
{
    if( Idx >= argv.size() ) return Default;
    else                     return __get_const_char(argv[Idx]);
}

template <class T> __GETPOT_INLINE T 
GetPot::get(const StringOrCharP VarName)
{
  return get<T>(VarName, (const char*)0x0);
}

template <class T> __GETPOT_INLINE T 
GetPot::get(const StringOrCharP VarName, const char* Constraint)
{
  return __get<T>(VarName, Constraint, /* ThrowExceptionF = */true); 
}

template <class T> __GETPOT_INLINE T  
GetPot::get(const StringOrCharP VarName, const char* Constraint, T Default)
{ 
    return __get<T>(VarName, Constraint, Default, /* ThrowExceptionF = */GETPOT_SETTING_THROW_EXCEPTION_ON_DEFAULT); 
}

__GETPOT_INLINE unsigned
GetPot::size() const
{ return static_cast<unsigned int>(argv.size()); }


//     -- next() function group
template <class T> __GETPOT_INLINE T
GetPot::next(T Default)
{
    if( search_failed_f ) return Default;
    cursor++;
    if( cursor >= argv.size() )  
    { cursor = static_cast<unsigned int>(argv.size()); return Default; }

    // (*) record requested argument for later ufo detection
    __record_argument_request(argv[cursor]);

    const std::string Remain = __get_remaining_string(argv[cursor], prefix);

    return Remain != "" ? __convert_to_type(Remain, (typename TypeInfo<T>::type)Default) : Default;
}

//     -- follow() function group
//        distinct option to be searched for
template <class T> __GETPOT_INLINE T
GetPot::follow(T Default, const char* Option)
{
    // (*) record requested of argument is entirely handled in 'search()' and 'next()'
    if( search(Option) == false ) return Default;
    return next(Default);
}


//     -- second follow() function group
//        multiple option to be searched for
template <class T> __GETPOT_INLINE T
GetPot::follow(T Default, unsigned No, const char* P, ...)
{
    // (*) record requested of argument is entirely handled in 'search()' and 'next()'
    if( No == 0 ) return Default;
    if( search(P) == true ) return next(Default);

    va_list ap;
    va_start(ap, P);
    unsigned i=1;
    for(; i<No; ++i) {
        char* Opt = va_arg(ap, char *);
        if( search(Opt) == true ) {
            va_end(ap);
            return next(Default);
        }
    }
    va_end(ap);
    return Default;
}

///////////////////////////////////////////////////////////////////////////////
// (*) directly connected options
//.............................................................................
//
template <class T> __GETPOT_INLINE T
GetPot::direct_follow(T Default, const char* Option)
{
    const char* FollowStr = __match_starting_string(Option);
    if( FollowStr == 0x0 )  return Default;

    // (*) record requested of argument for later ufo-detection
    __record_argument_request(std::string(Option) + FollowStr);

    if( ++cursor >= static_cast<unsigned int>(argv.size()) ) cursor = static_cast<unsigned int>(argv.size());
    return __convert_to_type(FollowStr, (typename TypeInfo<T>::type)Default);
}

__GETPOT_INLINE std::vector<std::string>
GetPot::string_tails(const char* StartString)
{
    std::vector<std::string>  result;
    const unsigned            N = static_cast<unsigned int>(strlen(StartString));

    std::vector<std::string>::iterator it = argv.begin();

    unsigned idx = 0;
    while( it != argv.end() ) {
        // (*) does start string match the given option?
        //     NO -> goto next option
        if( (*it).compare(0, N, StartString) != 0 ) { ++it; ++idx; continue; }

        // append the found tail to the result vector
        result.push_back((*it).substr(N));
                
        // adapt the nominus vector
        std::vector<unsigned>::iterator nit = idx_nominus.begin();
        for(; nit != idx_nominus.end(); ++nit) {
            if( *nit == idx ) {
                idx_nominus.erase(nit);
                for(; nit != idx_nominus.end(); ++nit) *nit -= 1;
                break;
            }
        }
        
        // erase the found option
        argv.erase(it);

        // 100% safe solution: set iterator back to the beginning.
        // (normally, 'it--' would be enough, but who knows how the
        // iterator is implemented and .erase() definitely invalidates
        // the current iterator position.
        if( argv.empty() ) break;
        it = argv.begin();
    }
    cursor = 0;
    nominus_cursor = -1;
    return result;
}

__GETPOT_INLINE std::vector<int>
GetPot::int_tails(const char* StartString, const int Default /* = -1 */)
{
    std::vector<int>  result;
    const unsigned    N = static_cast<unsigned int>(strlen(StartString));

    std::vector<std::string>::iterator it = argv.begin();

    unsigned idx = 0;
    while( it != argv.end() ) {
        // (*) does start string match the given option?
        //     NO -> goto next option
        if( (*it).compare(0, N, StartString) != 0 ) { ++it; ++idx; continue; }
            
        // append the found tail to the result vector
        result.push_back(__convert_to_type((*it).substr(N), Default));

        // adapt the nominus vector
        std::vector<unsigned>::iterator nit = idx_nominus.begin();
        for(; nit != idx_nominus.end(); ++nit) {
            if( *nit == idx ) {
                idx_nominus.erase(nit);
                for(; nit != idx_nominus.end(); ++nit) *nit -= 1;
                break;
            }
        }

        // erase the found option
        argv.erase(it);
        
        // 100% safe solution: set iterator back to the beginning.
        // (normally, 'it--' would be enough, but who knows how the
        // iterator is implemented and .erase() definitely invalidates
        // the current iterator position.
        if( argv.empty() ) break;
        it = argv.begin();
    }
    cursor = 0;
    nominus_cursor = -1;
    return result;
}

__GETPOT_INLINE std::vector<double>
GetPot::double_tails(const char*  StartString, 
                     const double Default /* = -1.0 */)
{
    std::vector<double>  result;
    const unsigned       N = static_cast<unsigned int>(strlen(StartString));

    std::vector<std::string>::iterator it = argv.begin();
    unsigned                           idx = 0;
    while( it != argv.end() ) {
        // (*) does start string match the given option?
        //     NO -> goto next option
        if( (*it).compare(0, N, StartString) != 0 ) { ++it; ++idx; continue; }
            
        // append the found tail to the result vector
        result.push_back(__convert_to_type((*it).substr(N), Default));

        // adapt the nominus vector
        std::vector<unsigned>::iterator nit = idx_nominus.begin();
        for(; nit != idx_nominus.end(); ++nit) {
            if( *nit == idx ) {
                idx_nominus.erase(nit);
                for(; nit != idx_nominus.end(); ++nit) *nit -= 1;
                break;
            }
        }

        // erase the found option
        argv.erase(it);
        
        // 100% safe solution: set iterator back to the beginning.
        // (normally, 'it--' would be enough, but who knows how the
        // iterator is implemented and .erase() definitely invalidates
        // the current iterator position.
        if( argv.empty() ) break;
        it = argv.begin();
    }
    cursor = 0;
    nominus_cursor = -1;
    return result;
}

///////////////////////////////////////////////////////////////////////////////
// (*) lists of nominus following an option
//
__GETPOT_INLINE std::vector<std::string> 
GetPot::nominus_followers(const char* Option)
{
    std::vector<std::string>  result_list;
    if( search(Option) == false ) return result_list;
    while( 1 + 1 == 2 ) {
        ++cursor;
        if( cursor >= argv.size() ) { 
            cursor = argv.size() - 1;
            return result_list;
        }
        if( argv[cursor].length() >= 1 ) {
            if( argv[cursor][0] == '-' ) {
                return result_list;
            }
            // -- record for later ufo-detection
            __record_argument_request(argv[cursor]);
            // -- append to the result list
            result_list.push_back(argv[cursor]);
        }
    }
}

__GETPOT_INLINE std::vector<std::string> 
GetPot::nominus_followers(unsigned No, ...)
{
    std::vector<std::string>  result_list;
    // (*) record requested of argument is entirely handled in 'search()' 
    //     and 'nominus_followers()'
    if( No == 0 ) return result_list;

    va_list ap;
    va_start(ap, No);   
    for(unsigned i=0; i<No; ++i) {
        char* Option = va_arg(ap, char *);
        std::vector<std::string> tmp = nominus_followers(Option);
        result_list.insert(result_list.end(), tmp.begin(), tmp.end());

        // std::cerr << "option = '" << Option << "'" << std::endl;
        // std::cerr << "length = " << tmp.size() << std::endl;
        // std::cerr << "new result list = <";
        // for(std::vector<std::string>::const_iterator it = result_list.begin();
        //    it != result_list.end(); ++it) 
        //    std::cerr << *it << ", ";
        // std::cerr << ">\n";
    }
    va_end(ap);
    return result_list;
}


__GETPOT_INLINE const char*
GetPot::__match_starting_string(const char* StartString)
    // pointer  to the place where the string after
    //          the match inside the found argument starts.
    // 0        no argument matches the starting string.
{
    const unsigned N         = static_cast<unsigned int>(strlen(StartString));
    unsigned       OldCursor = cursor;
    // const char*    tmp       = (const char*)0;

    if( OldCursor >= static_cast<unsigned int>(argv.size()) ) OldCursor = static_cast<unsigned int>(argv.size()) - 1;
    search_failed_f = true;

    // (*) first loop from cursor position until end
    unsigned c = cursor;
    for(; c < argv.size(); c++) {
        if( argv[c].compare(0, N, StartString) == 0 ) { 
            cursor          = c; 
            search_failed_f = false; 
            return __get_const_char(argv[c].substr(N));
        }
    }

    if( ! search_loop_f ) return (const char*)0;

    // (*) second loop from 0 to old cursor position
    for(c = 1; c < OldCursor; c++) {
        if( argv[c].compare(0, N, StartString) == 0 ) { 
            cursor          = c; 
            search_failed_f = false; 
            return __get_const_char(argv[c].substr(N));
        }
    }
    return 0;
}

///////////////////////////////////////////////////////////////////////////////
// (*) search for flags
//.............................................................................
//
__GETPOT_INLINE bool
GetPot::options_contain(const char* FlagList) const
{
    // go through all arguments that start with a '-' (but not '--')
    std::string str;
    STRING_VECTOR::const_iterator it = argv.begin();
    for(; it != argv.end(); ++it) {
        str = __get_remaining_string(*it, prefix);

        if( str.length() >= 2 && str[0] == '-' && str[1] != '-' )
            if( __check_flags(str, FlagList) ) return true;
    }
    return false;
}

__GETPOT_INLINE bool
GetPot::argument_contains(unsigned Idx, const char* FlagList) const
{
    if( Idx >= argv.size() ) return false;

    // (*) record requested of argument for later ufo-detection
    //     an argument that is checked for flags is considered to be 'requested'
    ((GetPot*)this)->__record_argument_request(argv[Idx]);

    if( prefix == "" )
        // search argument for any flag in flag list
        return __check_flags(argv[Idx], FlagList);

    // if a prefix is set, then the argument index is the index
    //   inside the 'namespace'
    // => only check list of arguments that start with prefix
    unsigned no_matches = 0;
    unsigned i=0;
    for(; i<argv.size(); ++i) {
        const std::string Remain = __get_remaining_string(argv[i], prefix);
        if( Remain != "") {
            no_matches += 1;
            if( no_matches == Idx)
                return __check_flags(Remain, FlagList);
        }
    }
    // no argument in this namespace
    return false;
}

__GETPOT_INLINE bool
GetPot::__check_flags(const std::string& Str, const char* FlagList) const
{
    const char* p=FlagList;
    for(; *p != '\0' ; p++)
        if( Str.find(*p) != std::string::npos ) return true; // found something
    return false;
}

///////////////////////////////////////////////////////////////////////////////
// (*) nominus arguments
__GETPOT_INLINE STRING_VECTOR
GetPot::nominus_vector() const
    // return vector of nominus arguments
{
    STRING_VECTOR nv;
    std::vector<unsigned>::const_iterator it = idx_nominus.begin();
    for(; it != idx_nominus.end(); ++it) {
        nv.push_back(argv[*it]);

        // (*) record for later ufo-detection
        //     when a nominus vector is requested, the entire set of nominus arguments are 
        //     tagged as 'requested'
        ((GetPot*)this)->__record_argument_request(argv[*it]);
    }
    return nv;
}

__GETPOT_INLINE std::string
GetPot::next_nominus()
{
    if( nominus_cursor < int(idx_nominus.size()) - 1 ) {
        const std::string Tmp = argv[idx_nominus[++nominus_cursor]];
        __record_argument_request(Tmp);
        return Tmp; 
    }
    return std::string("");
}

///////////////////////////////////////////////////////////////////////////////
// (*) variables
//.............................................................................
//
template <class T> __GETPOT_INLINE T 
GetPot::__get(const StringOrCharP VarName, const char* Constraint, bool ThrowExceptionF) const
{
    T Default = TypeInfo<T>::default_value();

    const variable*  sv = __find_variable(VarName.content, TypeInfo<T>::name(), ThrowExceptionF);

    if( sv == 0x0 ) 
        return Default;

    if( Constraint != 0x0 && ! __constraint_check(sv->original, Constraint, ThrowExceptionF) ) 
        return Default;

    return __convert_to_type<typename TypeInfo<T>::type>(sv->original, TypeInfo<T>::convert(Default), ThrowExceptionF);
}

template <class T> __GETPOT_INLINE T 
GetPot::__get(const StringOrCharP VarName, const char* Constraint, T Default, bool ThrowExceptionF) const
{
    const variable*  sv = __find_variable(VarName.content, TypeInfo<T>::name(), false);

    if( sv == 0x0 ) 
        return Default;

    if( Constraint != 0x0 && ! __constraint_check(sv->original, Constraint, ThrowExceptionF) ) 
        return Default;

    return __convert_to_type<typename TypeInfo<T>::type>(sv->original, TypeInfo<T>::convert(Default), ThrowExceptionF);
}

template <class T> __GETPOT_INLINE T 
GetPot::__get_element(const StringOrCharP VarName, unsigned Idx, const char* Constraint,
                      bool ThrowExceptionF) const
{
    T Default = TypeInfo<T>::default_value();

    const variable* sv = __find_variable(VarName.content, TypeInfo<T>::name(), ThrowExceptionF);

    if( sv == 0 )       
        return Default;

    const std::string*  element = sv->get_element(Idx, ThrowExceptionF);

    if( element == 0 )  
        return Default;

    if( Constraint != 0x0 && ! __constraint_check(*element, Constraint, ThrowExceptionF) ) 
        return Default;

    return __convert_to_type<typename TypeInfo<T>::type>(*element, TypeInfo<T>::convert(Default), ThrowExceptionF);
}

template <class T> __GETPOT_INLINE T 
GetPot::__get_element(const StringOrCharP VarName, unsigned Idx, const char* Constraint, 
                               T Default, bool ThrowExceptionF) const
{
    const variable* sv = __find_variable(VarName.content, TypeInfo<T>::name(), false);

    if( sv == 0 )       
        return Default;

    const std::string*  element = sv->get_element(Idx, ThrowExceptionF);

    if( element == 0 )  
        return Default;

    else if( Constraint != 0x0 && ! __constraint_check(*element, Constraint, ThrowExceptionF) ) 
        return Default;

    return __convert_to_type<typename TypeInfo<T>::type>(*element, TypeInfo<T>::convert(Default), ThrowExceptionF);
}

template<class T> __GETPOT_INLINE T
GetPot::operator()(const StringOrCharP VarName, T Default) const
{ return __get<T>(VarName, (const char*)0x0, Default, /* ThrowExceptionF = */GETPOT_SETTING_THROW_EXCEPTION_ON_DEFAULT); }

template <class T> __GETPOT_INLINE T 
GetPot::get_element(const StringOrCharP VarName, unsigned Idx, const char* Constraint)
{ return __get_element<T>(VarName, Idx, Constraint, /* ThrowExceptionF = */true); }

template <class T> __GETPOT_INLINE T  
GetPot::get_element(const StringOrCharP VarName, unsigned Idx, const char* Constraint, T Default)
{ return __get_element<T>(VarName, Idx, Constraint, Default, /* ThrowExceptionF = */GETPOT_SETTING_THROW_EXCEPTION_ON_DEFAULT); }

template <class T> __GETPOT_INLINE T
GetPot::operator()(const StringOrCharP VarName, unsigned Idx, T Default) const
{ return __get_element<T>(VarName, Idx, (const char*)0x0, Default, /* ThrowExceptionF = */GETPOT_SETTING_THROW_EXCEPTION_ON_DEFAULT); }


__GETPOT_INLINE void 
GetPot::__record_argument_request(const std::string& Name)
{
    if( ! __request_recording_f ) return; 

    // (*) record requested variable for later ufo detection
    _requested_arguments.push_back(Name);

    // (*) record considered section for ufo detection
    STRING_VECTOR      STree = __get_section_tree(Name);
    victorate(std::string, STree, it) {
        if( find(_requested_sections.begin(), _requested_sections.end(), *it) == _requested_sections.end() ) {
            if( section.length() != 0 ) _requested_sections.push_back(*it);
        }
    }
}

__GETPOT_INLINE void 
GetPot::__record_variable_request(const std::string& Name)
{
    if( ! __request_recording_f ) return; 

    // (*) record requested variable for later ufo detection
    _requested_variables.push_back(Name);

    // (*) record considered section for ufo detection
    STRING_VECTOR      STree = __get_section_tree(Name);
    victorate(std::string, STree, it) {
        if( find(_requested_sections.begin(), _requested_sections.end(), *it) == _requested_sections.end() )
            if( section.length() != 0 ) _requested_sections.push_back(*it);
    }
}

// To build a GetPot instance

__GETPOT_INLINE void
GetPot::__build(const std::string& FileName,
                const std::string& CommentStart, const std::string& CommentEnd,
                const std::string& FieldSeparator)
{
    filename = FileName;
    __basic_initialization();

    // if specified -> overwrite default strings
    if( CommentStart.length()   != 0 ) _comment_start   = std::string(CommentStart);
    if( CommentEnd.length()     != 0 ) _comment_end     = std::string(CommentEnd);
    if( FieldSeparator.length() != 0 ) _field_separator = FieldSeparator;

    STRING_VECTOR _apriori_argv;
    // -- file name is element of argument vector, however, it is not parsed for
    //    variable assignments or nominuses.
    _apriori_argv.push_back(std::string(FileName));

    STRING_VECTOR args = __read_in_file(FileName);
    _apriori_argv.insert(_apriori_argv.begin()+1, args.begin(), args.end());
    __parse_argument_vector(_apriori_argv);
}

// to split a string according to delimiters and elements are stored in a vector.
__GETPOT_INLINE
void GetPot::__split(std::string str, std::vector<std::string>& vect,
                     std::string delimiters /*= " \n\t"*/)
{
    vect.clear();

    std::string tmp;
    std::string::size_type index_beg, index_end;

    index_beg = str.find_first_not_of(delimiters);

    while (index_beg != std::string::npos) {
        index_end = str.find_first_of(delimiters, index_beg);
        tmp = str.substr(index_beg, index_end == std::string::npos ?
                         std::string::npos : (index_end - index_beg));
        vect.push_back(tmp);
        index_beg = str.find_first_not_of(delimiters, index_end);
    }
}

// (*) following functions are to be used from 'outside', after getpot has parsed its
//     arguments => append an argument in the argument vector that reflects the addition
__GETPOT_INLINE void
GetPot::__set_variable(const std::string& VarName, const std::string& Value)
{
    const GetPot::variable* Var;
    const std::string       EmptyString("");

    if ( !built )
      Var = __find_variable(VarName, EmptyString, false);
    else
      Var = __find_variable(VarName, EmptyString);

    if( Var == 0 ) variables.push_back(variable(VarName, Value, _field_separator));
    else           ((GetPot::variable*)Var)->take(Value, _field_separator);
}

template <>__GETPOT_INLINE void
GetPot::set_variable<const char*>(const StringOrCharP VarName, const char* Value, const bool Requested /* = true*/)
{     
    const std::string Arg = prefix + VarName.content + std::string("=") + std::string(Value);
    argv.push_back(Arg);
    __set_variable(VarName.content, Value);

    // if user does not specify the variable as 'not being requested' it will be 
    // considered amongst the requested variables
    if( Requested ) __record_variable_request(Arg);
}

template <> __GETPOT_INLINE void
GetPot::set_variable<double>(const StringOrCharP VarName, double Value, const bool Requested /* = true*/)
{ __set_variable(VarName.content, __double2string(Value)); }

template <> __GETPOT_INLINE void 
GetPot::set_variable<int>(const StringOrCharP VarName, int Value, const bool Requested /* = true*/)
{ __set_variable(VarName.content, __int2string(Value)); }

template <> __GETPOT_INLINE void 
GetPot::set_variable<bool>(const StringOrCharP VarName, bool Value, const bool Requested /* = true*/)
{ 
    if( true_string_list.size() == 0 || false_string_list.size() == 0 ) {
        throw std::runtime_error(GETPOT_STR_TRUE_FALSE_UNDEFINED);
    }
    __set_variable(VarName.content, 
                   Value ? true_string_list[0] : false_string_list[0]); 
}

template <class T>
__GETPOT_INLINE void
GetPot::set(const StringOrCharP VarName, T& Value)
{
  Value = get<T>(VarName);
}

template <class T>
__GETPOT_INLINE void
GetPot::set(const StringOrCharP VarName, T& Value, const char* Constraint)
{
  Value = get<T>(VarName, Constraint);
}

template <class T, class U>
__GETPOT_INLINE void
GetPot::set(const StringOrCharP VarName, T& Value, const char* Constraint, U Default)
{
  Value = get<U>(VarName, Constraint, Default);
}

__GETPOT_INLINE unsigned
GetPot::vector_variable_size(StringOrCharP VarName) const
{
    const std::string   EmptyString("");

    const variable*  sv = __find_variable(VarName.content, EmptyString);
    if( sv == 0 ) return 0;
    return static_cast<unsigned int>(sv->value.size());
}

__GETPOT_INLINE STRING_VECTOR
GetPot::get_variable_names() const
{
    STRING_VECTOR  result;
    std::vector<GetPot::variable>::const_iterator it = variables.begin();
    for(; it != variables.end(); ++it) {
        const std::string Tmp = __get_remaining_string((*it).name, prefix);
        if( Tmp != "" ) result.push_back(Tmp);
    }
    return result;
}

__GETPOT_INLINE STRING_VECTOR
GetPot::get_section_names() const
{ return section_list; }

__GETPOT_INLINE const GetPot::variable*
GetPot::__find_variable(const std::string& VarName, const std::string& TypeName,
                        bool ThrowExceptionF/*=GETPOT_SETTING_THROW_EXCEPTION_ON_DEFAULT*/) const
{
    const std::string Name = prefix + VarName;

    // (*) record requested variable for later ufo detection
    ((GetPot*)this)->__record_variable_request(Name);

    /* Linear search (?)... maybe we can do something better ... */
    std::vector<variable>::const_iterator it = variables.begin();
    for(; it != variables.end(); ++it) {
        if( (*it).name == Name ) return &(*it);
    }

    if( ThrowExceptionF ) {
        std::string  msg(GETPOT_STR_FILE " ");
        msg += std::string("\"") + filename + "\": ";
        msg += std::string(GETPOT_STR_MISSING_VARIABLE) + " \'";
        msg += VarName + "\' ";
        if( TypeName.length() != 0 ) msg += std::string(GETPOT_STR_WITH_TYPE " ")  + TypeName;
        if( ! prefix.empty() )       msg += std::string(" " GETPOT_STR_IN_SECTION " \"") + prefix + "\"";
        msg += ".";
        throw std::runtime_error(msg);
    }
    else {
        return 0x0;
    }
}

///////////////////////////////////////////////////////////////////////////////
// (*) ouput (basically for debugging reasons
//.............................................................................
//
__GETPOT_INLINE int
GetPot::print() const
{
    std::cout << "argc = " << static_cast<unsigned int>(argv.size()) << std::endl;
    STRING_VECTOR::const_iterator it = argv.begin();
    for(; it != argv.end(); ++it)
        std::cout << *it << std::endl;
    std::cout << std::endl;
    return 1;
}

// (*) dollar bracket expressions (DBEs) ------------------------------------
//
//     1) Entry Function: __DBE_expand_string()
//        Takes a string such as
//
//          "${+ ${x} ${y}}   Subject-${& ${section} ${subsection}}:   ${title}"
//
//        calls __DBE_expand() for each of the expressions
//
//           ${+ ${x} ${y}}
//           ${& ${section} ${subsection}}
//           ${Title}
//
//        and returns the string
//
//          "4711 Subject-1.01:   Mit den Clowns kamen die Schwaene"
//
//        assuming that
//            x          = "4699"
//            y          = "12"
//            section    = "1."
//            subsection = "01"
//            title      = "Mit den Clowns kamen die Schwaene"
//
//      2) __DBE_expand():
//
//           checks for the command, i.e. the 'sign' that follows '${'
//           divides the argument list into sub-expressions using
//           __DBE_get_expr_list()
//
//           ${+ ${x} ${y}}                 -> "${x}"  "${y}"
//           ${& ${section} ${subsection}}  -> "${section}" "${subsection}"
//           ${Title}                       -> Nothing, variable expansion
//
//      3) __DBE_expression_list():
//
//           builds a vector of unbracketed whitespace separated strings, i.e.
//
//           "  ${Number}.a ${: Das Marmorbild} AB-${& Author= ${Eichendorf}-1870}"
//
//           is split into a vector
//
//              [0] ${Number}.a
//              [1] ${: Das Marmorbild}
//              [2] AB-${& Author= ${Eichendorf}}-1870
//
//           Each sub-expression is expanded using expand().
//---------------------------------------------------------------------------
__GETPOT_INLINE std::string
GetPot::__DBE_expand_string(const std::string str)
{
    // Parses for closing operators '${ }' and expands them letting
    // white spaces and other letters as they are.
    std::string   new_string = "";
    unsigned open_brackets = 0;
    unsigned first = 0;
    unsigned i = 0;
    for(;  i<str.size(); ++i) {
        if( i < str.size() - 2 && str.substr(i, 2) == "${" ) {
            if( open_brackets == 0 ) first = i+2;
            open_brackets++;
        }
        else if( str[i] == '}' && open_brackets > 0) {
            open_brackets -= 1;
            if( open_brackets == 0 ) {
                const std::string Replacement = __DBE_expand(str.substr(first, i - first));
                new_string += Replacement;
            }
        }
        else if( open_brackets == 0 )
            new_string += str[i];
    }
    return new_string;
}

__GETPOT_INLINE STRING_VECTOR
GetPot::__DBE_get_expr_list(const std::string str_, const unsigned ExpectedNumber)
    // ensures that the resulting vector has the expected number
    // of arguments, but they may contain an error message
{
    std::string str = str_;
    // Separates expressions by non-bracketed whitespaces, expands them
    // and puts them into a list.

    unsigned i=0;
    // (1) eat initial whitespaces
    for(; i < str.size(); ++i)
        if( ! isspace(str[i]) ) break;

    STRING_VECTOR   expr_list;
    unsigned         open_brackets = 0;
    std::vector<unsigned> start_idx;
    unsigned         start_new_string = i;
    unsigned         l = static_cast<unsigned int>(str.size());

    // (2) search for ${ } expressions ...
    while( i < l ) {
        const char letter = str[i];
        // whitespace -> end of expression
        if( isspace(letter) && open_brackets == 0) {
            expr_list.push_back(str.substr(start_new_string, i - start_new_string));
            bool no_breakout_f = true;
            for(++i; i < l ; ++i) {
                if( ! isspace(str[i]) )
                { no_breakout_f = false; start_new_string = i; break; }
            }
            if( no_breakout_f ) {
                // end of expression list
                if( expr_list.size() < ExpectedNumber ) {
                    const std::string   pre_tmp("<< ${ }: missing arguments>>");
                    STRING_VECTOR tmp(ExpectedNumber - expr_list.size(), pre_tmp);
                    expr_list.insert(expr_list.end(), tmp.begin(), tmp.end());
                }
                return expr_list;
            }
        }

        // dollar-bracket expression
        if( str.length() >= i+2 && str.substr(i, 2) == "${" ) {
            open_brackets++;
            start_idx.push_back(i+2);
        }
        else if( letter == '}' && open_brackets > 0) {
            int start = start_idx[start_idx.size()-1];
            start_idx.pop_back();
            const std::string Replacement = __DBE_expand(str.substr(start, i-start));
            if( start - 3 < (int)0)
                str = Replacement + str.substr(i+1);
            else
                str = str.substr(0, start-2) + Replacement + str.substr(i+1);
            l = static_cast<unsigned int>(str.size());
            i = start + static_cast<unsigned int>(Replacement.size()) - 3;
            open_brackets--;
        }
        ++i;
    }

    // end of expression list
    expr_list.push_back(str.substr(start_new_string, i-start_new_string));

    if( expr_list.size() < ExpectedNumber ) {
        const std::string   pre_tmp("<< ${ }: missing arguments>>");
        STRING_VECTOR tmp(ExpectedNumber - expr_list.size(), pre_tmp);
        expr_list.insert(expr_list.end(), tmp.begin(), tmp.end());
    }

    return expr_list;
}

__GETPOT_INLINE const GetPot::variable*
GetPot::__DBE_get(std::string VarName)
{
    static GetPot::variable ev;
    const  std::string      EmptyString("");

    std::string secure_Prefix = prefix;

    prefix = section;
    // (1) first search in currently active section
    const GetPot::variable* var = __find_variable(VarName, EmptyString, false);
    if( var != 0 ) { prefix = secure_Prefix; return var; }

    // (2) search in root name space
    prefix = "";
    var = __find_variable(VarName, EmptyString);
    if( var != 0 ) { prefix = secure_Prefix; return var; }

    prefix = secure_Prefix;

    // error occured => variable name == ""
    char* tmp = new char[VarName.length() + 25];
    GETPOT_SNPRINTF(tmp, (int)sizeof(char)*(VarName.length() + 25), 
                    "<<${ } variable '%s' undefined>>", VarName.c_str());
    ev.name = "";
    ev.original = std::string(tmp);
    delete [] tmp;
    return &ev;
}

__GETPOT_INLINE std::string
GetPot::__DBE_expand(const std::string expr)
{
    // ${: } pure text
    if( expr[0] == ':' )
        return expr.substr(1);

    // ${& expr expr ... } text concatination
    else if( expr[0] == '&' ) {
        const STRING_VECTOR A = __DBE_get_expr_list(expr.substr(1), 1);

        STRING_VECTOR::const_iterator it = A.begin();
        std::string result = *it++;
        for(; it != A.end(); ++it) result += *it;

        return result;
    }

    // ${<-> expr expr expr} text replacement
    else if( expr.length() >= 3 && expr.substr(0, 3) == "<->" ) {
        STRING_VECTOR A = __DBE_get_expr_list(expr.substr(3), 3);
        std::string::size_type tmp = 0;
        const std::string::size_type L = A[1].length();
        while( (tmp = A[0].find(A[1])) != std::string::npos ) {
            A[0].replace(tmp, L, A[2]);
        }
        return A[0];
    }
    // ${+ ...}, ${- ...}, ${* ...}, ${/ ...} expressions
    else if( expr[0] == '+' ) {
        STRING_VECTOR A = __DBE_get_expr_list(expr.substr(1), 2);
        STRING_VECTOR::const_iterator it = A.begin();
        double result = __convert_to_type(*it++, 0.0);
        for(; it != A.end(); ++it)
            result += __convert_to_type(*it, 0.0);

        return __double2string(result);
    }
    else if( expr[0] == '-' ) {
        STRING_VECTOR A = __DBE_get_expr_list(expr.substr(1), 2);
        STRING_VECTOR::const_iterator it = A.begin();
        double result = __convert_to_type(*it++, 0.0);
        for(; it != A.end(); ++it)
            result -= __convert_to_type(*it, 0.0);

        return __double2string(result);
    }
    else if( expr[0] == '*' ) {
        STRING_VECTOR A = __DBE_get_expr_list(expr.substr(1), 2);
        STRING_VECTOR::const_iterator it = A.begin();
        double result = __convert_to_type(*it++, 0.0);
        for(; it != A.end(); ++it)
            result *= __convert_to_type(*it, 0.0);

        return __double2string(result);
    }
    else if( expr[0] == '/' ) {

        STRING_VECTOR A = __DBE_get_expr_list(expr.substr(1), 2);
        STRING_VECTOR::const_iterator it = A.begin();
        double result = __convert_to_type(*it++, 0.0);
        if( result == 0 ) return "0.0";
        for(; it != A.end(); ++it) {
            const double Q = __convert_to_type(*it, 0.0);
            if( Q == 0.0 ) return "0.0";
            result /= Q;
        }
        return __double2string(result);
    }

    // ${^ ... } power expressions
    else if( expr[0] == '^' ) {
        STRING_VECTOR A = __DBE_get_expr_list(expr.substr(1), 2);
        STRING_VECTOR::const_iterator it = A.begin();
        double result = __convert_to_type(*it++, 0.0);
        for(; it != A.end(); ++it)
            result = pow(result, __convert_to_type(*it, 0.0));
        return __double2string(result);
    }

    // ${==  } ${<=  } ${>= } comparisons (return the number of the first 'match'
    else if( expr.length() >= 2 &&
             ( expr.substr(0,2) == "==" || expr.substr(0,2) == ">=" ||
               expr.substr(0,2) == "<=" || expr[0] == '>'           || expr[0] == '<')) {
        // differentiate between two and one sign operators
        unsigned op = 0;
        enum { EQ, GEQ, LEQ, GT, LT };
        if      ( expr.substr(0, 2) == "==" ) op = EQ;
        else if ( expr.substr(0, 2) == ">=" ) op = GEQ;
        else if ( expr.substr(0, 2) == "<=" ) op = LEQ;
        else if ( expr[0] == '>' )            op = GT;
        else    /*                     "<" */ op = LT;

        STRING_VECTOR a;
        if ( op == GT || op == LT ) a = __DBE_get_expr_list(expr.substr(1), 2);
        else                        a = __DBE_get_expr_list(expr.substr(2), 2);

        std::string   x_orig = a[0];
        double   x = __convert_to_type(x_orig, 1e37);
        unsigned i = 1;

        STRING_VECTOR::const_iterator y_orig = a.begin();
        for(y_orig++; y_orig != a.end(); y_orig++) {
            double y = __convert_to_type(*y_orig, 1e37);

            // set the strings as reference if one wasn't a number
            if ( x == 1e37 || y == 1e37 ) {
                // it's a string comparison
                if( (op == EQ  && x_orig == *y_orig) || (op == GEQ && x_orig >= *y_orig) ||
                    (op == LEQ && x_orig <= *y_orig) || (op == GT  && x_orig >  *y_orig) ||
                    (op == LT  && x_orig <  *y_orig) )
                    return __int2string(i);
            }
            else {
                // it's a number comparison
                if( (op == EQ  && x == y) || (op == GEQ && x >= y) ||
                    (op == LEQ && x <= y) || (op == GT  && x >  y) ||
                    (op == LT  && x <  y) )
                    return __int2string(i);
            }
            ++i;
        }

        // nothing fulfills the condition => return 0
        return "0";
    }
    // ${?? expr expr} select
    else if( expr.length() >= 2 && expr.substr(0, 2) == "??" ) {
        STRING_VECTOR a = __DBE_get_expr_list(expr.substr(2), 2);
        double x = __convert_to_type(a[0], 1e37);
        // last element is always the default argument
        if( x == 1e37 || x < 0 || x >= a.size() - 1 ) return a[a.size()-1];

        // round x to closest integer
        return a[int(x+0.5)];
    }
    // ${? expr expr expr} if then else conditions
    else if( expr[0] == '?' ) {
        STRING_VECTOR a = __DBE_get_expr_list(expr.substr(1), 2);
        if( __convert_to_type(a[0], 0.0) == 1.0 ) return a[1];
        else if( a.size() > 2 ) return a[2];
    }
    // ${! expr} maxro expansion
    else if( expr[0] == '!' ) {
        const GetPot::variable* Var = __DBE_get(expr.substr(1));
        // error
        if( Var->name == "" ) return std::string(Var->original);

        const STRING_VECTOR A = __DBE_get_expr_list(Var->original, 2);
        return A[0];
    }
    // ${@: } - string subscription
    else if( expr.length() >= 2 && expr.substr(0,2) == "@:" ) {
        const STRING_VECTOR A = __DBE_get_expr_list(expr.substr(2), 2);
        double x = __convert_to_type(A[1], 1e37);

        // last element is always the default argument
        if( x == 1e37 || x < 0 || x >= A[0].size() - 1)
            return "<<1st index out of range>>";

        if( A.size() > 2 ) {
            double y = __convert_to_type(A[2], 1e37);
            if ( y != 1e37 && y > 0 && y <= A[0].size() - 1 && y > x )
                return A[0].substr(int(x+0.5), int(y+1.5) - int(x+0.5));
            else if( y == -1 )
                return A[0].substr(int(x+0.5));
            return "<<2nd index out of range>>";
        }
        else {
            char* tmp = new char[2];
            tmp[0] = A[0][int(x+0.5)]; tmp[1] = '\0';
            std::string result(tmp);
            delete [] tmp;
            return result;
        }
    }
    // ${@ } - vector subscription
    else if( expr[0] == '@' ) {
        STRING_VECTOR          A   = __DBE_get_expr_list(expr.substr(1), 2);
        const GetPot::variable* Var = __DBE_get(A[0]);
        // error
        if( Var->name == "" ) {
            // make a copy of the string if an error occured
            // (since the error variable is a static variable inside get())
            return std::string(Var->original);
        }

        double x = __convert_to_type(A[1], 1e37);

        // last element is always the default argument
        if (x == 1e37 || x < 0 || x >= Var->value.size() )
            return "<<1st index out of range>>";

        if ( A.size() > 2) {
            double y = __convert_to_type(A[2], 1e37);
            int    begin = int(x+0.5);
            int    end = 0;
            if ( y != 1e37 && y > 0 && y <= Var->value.size() && y > x)
                end = int(y+1.5);
            else if( y == -1 )
                end = static_cast<unsigned int>(Var->value.size());
            else
                return "<<2nd index out of range>>";

            std::string result = *(Var->get_element(begin));
            int i = begin+1;
            for(; i < end; ++i)
                result += std::string(" ") + *(Var->get_element(i));
            return result;
        }
        else
            return *(Var->get_element(int(x+0.5)));
    }

    const STRING_VECTOR    A = __DBE_get_expr_list(expr, 1);
    const GetPot::variable* B = __DBE_get(A[0]);

    // make a copy of the string if an error occured
    // (since the error variable is a static variable inside get())
    if( B->name == "" ) return std::string(B->original);
    // (psuggs@pobox.com mentioned to me the warning MSVC++6.0 produces
    //  with:  else return B->original (thanks))
    return B->original;
}


///////////////////////////////////////////////////////////////////////////////
// (*) unidentified flying objects
//.............................................................................
//
__GETPOT_INLINE bool
GetPot::__search_string_vector(const STRING_VECTOR& VecStr, const std::string& Str) const
{
    victorate(std::string, VecStr, itk) {
        if( *itk == Str ) return true;
    }
    return false;
}

__GETPOT_INLINE STRING_VECTOR
GetPot::unidentified_arguments(unsigned Number,
                               const char* KnownArgument1, ...) const
{
    STRING_VECTOR known_arguments;

    // (1) create a vector of known arguments
    if( Number == 0 ) return STRING_VECTOR();

    va_list ap;
    va_start(ap, KnownArgument1);
    known_arguments.push_back(std::string(KnownArgument1));
    unsigned i=1;
    for(; i<Number; ++i)
        known_arguments.push_back(std::string(va_arg(ap, char *)));
    va_end(ap);

    return unidentified_arguments(known_arguments);
}

__GETPOT_INLINE STRING_VECTOR
GetPot::unidentified_arguments() const
{ return unidentified_arguments(_requested_arguments); }

__GETPOT_INLINE STRING_VECTOR
GetPot::unidentified_arguments(const STRING_VECTOR& Knowns) const
{
    STRING_VECTOR ufos;
    STRING_VECTOR::const_iterator it = argv.begin();
    ++it; // forget about argv[0] (application or filename)
    for(; it != argv.end(); ++it) {
        // -- argument belongs to prefixed section ?
        const std::string arg = __get_remaining_string(*it, prefix);
        if( arg == "" ) continue;

        // -- check if in list
        if( __search_string_vector(Knowns, arg) == false)
            ufos.push_back(*it);
    }
    return ufos;
}

__GETPOT_INLINE STRING_VECTOR
GetPot::unidentified_options(unsigned Number,
                             const char* KnownOption1, ...) const
{
    STRING_VECTOR known_options;

    // (1) create a vector of known arguments
    if( Number == 0 ) return STRING_VECTOR();

    va_list ap;
    va_start(ap, KnownOption1);
    known_options.push_back(std::string(KnownOption1));
    unsigned i=1;
    for(; i<Number; ++i)
        known_options.push_back(std::string(va_arg(ap, char *)));
    va_end(ap);

    return unidentified_options(known_options);
}

__GETPOT_INLINE STRING_VECTOR
GetPot::unidentified_options() const
{ 
    // -- every option is an argument. 
    // -- the set of requested arguments contains the set of requested options. 
    // -- IF the set of requested arguments contains unrequested options, 
    //    THEN they were requested as 'follow' and 'next' arguments and not as real options.
    //
    // => it is not necessary to separate requested options from the list
    STRING_VECTOR option_list;
    victorate(std::string, _requested_arguments, it) {
        const std::string arg = *it;
        if( arg.length() == 0 ) continue;
        if( arg[0] == '-' )     option_list.push_back(arg);
    }   
    return unidentified_options(option_list); 
}

__GETPOT_INLINE STRING_VECTOR
GetPot::unidentified_options(const STRING_VECTOR& Knowns) const
{
    STRING_VECTOR ufos;
    STRING_VECTOR::const_iterator it = argv.begin();
    ++it; // forget about argv[0] (application or filename)
    for(; it != argv.end(); ++it) {
        // -- argument belongs to prefixed section ?
        const std::string arg = __get_remaining_string(*it, prefix);
        if( arg == "" ) continue;

        // is argument really an option (starting with '-') ?
        if( arg.length() < 1 || arg[0] != '-' ) continue;

        if( __search_string_vector(Knowns, arg) == false)
            ufos.push_back(*it);
    }

    return ufos;
}

__GETPOT_INLINE std::string
GetPot::unidentified_flags(const char* KnownFlagList, int ArgumentNumber=-1) const
    // Two modes:
    //  ArgumentNumber >= 0 check specific argument
    //  ArgumentNumber == -1 check all options starting with one '-'
    //                       for flags
{
    std::string      ufos;
    STRING_VECTOR    known_arguments;
    std::string      KFL(KnownFlagList);

    // (2) iteration over '-' arguments (options)
    if( ArgumentNumber == -1 ) {
        STRING_VECTOR::const_iterator it = argv.begin();
        ++it; // forget about argv[0] (application or filename)
        for(; it != argv.end(); ++it) {
            // -- argument belongs to prefixed section ?
            const std::string arg = __get_remaining_string(*it, prefix);
            //
            // -- does arguments start with '-' (but not '--')
            if( arg.length() < 2 || arg[0] != '-' || arg[1] == '-' ) continue;

            // -- check out if flags inside option are contained in KnownFlagList
            for(ptrdiff_t i=1; i<arg.length(); ++i) {
                if( KFL.find(arg[i]) == std::string::npos ) ufos += arg[i];
            }
        }
    }
    // (1) check specific argument
    else {
        // -- only check arguments that start with prefix
        int no_matches = 0;
        unsigned i=1;
        for(; i<argv.size(); ++i) {
            const std::string Remain = __get_remaining_string(argv[i], prefix);
            if( Remain != "") {
                no_matches++;
                if( no_matches == ArgumentNumber) {
                    // -- the right argument number inside the section is found
                    // => check it for flags
                    for(ptrdiff_t i=1; i<Remain.length(); ++i) {
                        if( KFL.find(Remain[i]) == std::string::npos ) ufos += Remain[i];
                    }
                    return ufos;
                }
            }
        }
    }
    return ufos;
}

__GETPOT_INLINE STRING_VECTOR
GetPot::unidentified_variables(unsigned Number,
                               const char* KnownVariable1, ...) const
{
    STRING_VECTOR known_variables;

    // create vector of known arguments
    if( Number == 0 ) return STRING_VECTOR();

    va_list ap;
    va_start(ap, KnownVariable1);
    known_variables.push_back(std::string(KnownVariable1));
    unsigned i=1;
    for(; i<Number; ++i)
        known_variables.push_back(std::string(va_arg(ap, char *)));
    va_end(ap);

    return unidentified_variables(known_variables);
}

__GETPOT_INLINE STRING_VECTOR
GetPot::unidentified_variables(const STRING_VECTOR& Knowns) const
{
    STRING_VECTOR ufos;

    victorate(GetPot::variable, variables, it) {
        // -- check if variable has specific prefix
        const std::string var_name = __get_remaining_string((*it).name, prefix);
        if( var_name == "" ) continue;

        // -- check if variable is known
        if( __search_string_vector(Knowns, var_name) == false)
            ufos.push_back((*it).name);
    }
    return ufos;
}

__GETPOT_INLINE STRING_VECTOR
GetPot::unidentified_variables() const
{  return unidentified_variables(_requested_variables); }


__GETPOT_INLINE STRING_VECTOR
GetPot::unidentified_sections(unsigned Number,
                              const char* KnownSection1, ...) const
{
    STRING_VECTOR known_sections;

    // (1) create a vector of known arguments
    if( Number == 0 ) return STRING_VECTOR();

    va_list ap;
    va_start(ap, KnownSection1);
    known_sections.push_back(std::string(KnownSection1));
    unsigned i=1;
    for(; i<Number; ++i) {
        std::string tmp = std::string(va_arg(ap, char *));
        if( tmp.length() == 0 ) continue;
        if( tmp[tmp.length()-1] != '/' ) tmp += '/';
        known_sections.push_back(tmp);
    }
    va_end(ap);

    return unidentified_sections(known_sections);
}

__GETPOT_INLINE STRING_VECTOR
GetPot::unidentified_sections() const
{ return unidentified_sections(_requested_sections); }

__GETPOT_INLINE STRING_VECTOR
GetPot::unidentified_sections(const STRING_VECTOR& Knowns) const
{
    STRING_VECTOR ufos;

    victorate(std::string, section_list, it) {
        // -- check if section conform to prefix
        const std::string sec_name = __get_remaining_string(*it, prefix);
        if( sec_name == "" ) continue;

        // -- check if section is known
        if( __search_string_vector(Knowns, sec_name) == false )
            ufos.push_back(*it);
    }

    return ufos;
}


__GETPOT_INLINE STRING_VECTOR
GetPot::unidentified_nominuses(unsigned Number, const char* Known, ...) const
{
    STRING_VECTOR known_nominuses;

    // create vector of known arguments
    if( Number == 0 ) return STRING_VECTOR();

    va_list ap;
    va_start(ap, Known);
    known_nominuses.push_back(std::string(Known));
    unsigned i=1;
    for(; i<Number; ++i) {
        std::string tmp = std::string(va_arg(ap, char *));
        if( tmp.length() == 0 ) continue;
        known_nominuses.push_back(tmp);
    }
    va_end(ap);

    return unidentified_nominuses(known_nominuses);
}

__GETPOT_INLINE STRING_VECTOR
GetPot::unidentified_nominuses() const {
    // -- every nominus is an argument. 
    // -- the set of requested arguments contains the set of requested nominuss. 
    // -- IF the set of requested arguments contains unrequested nominuss, 
    //    THEN they were requested as 'follow' and 'next' arguments and not as real nominuses.
    //
    // => it is not necessary to separate requested nominus from the list

    return unidentified_nominuses(_requested_arguments);
}

__GETPOT_INLINE STRING_VECTOR
GetPot::unidentified_nominuses(const STRING_VECTOR& Knowns) const
{
    STRING_VECTOR ufos;

    // (2) iterate over all arguments
    STRING_VECTOR::const_iterator it = argv.begin();
    ++it; // forget about argv[0] (application or filename)
    for(; it != argv.end(); ++it) {
        // -- check if nominus part of prefix
        const std::string arg = __get_remaining_string(*it, prefix);
        if( arg == "" )                                         continue;

        if( arg.length() < 1 )                                  continue;
        // option ? --> not a nomius
        if( arg[0] == '-' )                                     continue;
        // section ? --> not a real nominus
        if( arg[0] == '[' && arg[arg.length()-1] == ']' )       continue;
        // variable definition ? --> not a real nominus
        bool continue_f = false;
        unsigned i=0;
        for(; i<arg.length() ; ++i)
            if( arg[i] == '=' ) { continue_f = true; break; }
        if( continue_f )                                        continue;

        // real nominuses are compared with the given list
        if( __search_string_vector(Knowns, arg) == false )
            ufos.push_back(*it);
    }
    return ufos;
}

bool 
GetPot::__constraint_check(const std::string& Value, const char* ConstraintStr, 
                           bool ThrowExceptionF) const
{
    std::string ConstraintString = std::string(ConstraintStr);
    if( ConstraintStr == (const char *) "" )           return true;
    if( __constraint_check_OR(Value, &ConstraintStr) ) return true;

    if( ThrowExceptionF ) {
        std::string  msg(GETPOT_STR_FILE " ");
        msg += std::string("\"") + filename + "\": \'";
        msg += std::string(Value) + "\' ";
        if( ! prefix.empty() ) 
            msg += std::string("\n " GETPOT_STR_IN_SECTION " \"") + prefix + "\"\n";
        msg += std::string(GETPOT_STR_DOES_NOT_SATISFY_CONSTRAINT) + " \'" + ConstraintString + "\'";
        msg += ".";
        throw std::runtime_error(msg);
    }
    else {
        return false;
    }
}

/*
 * GRAMMAR:
 *
 * or_expr:   or_expr '|' and_expr
 *            and_expr
 *
 * and_expr:  and_expr '&' primary
 *            primary
 *
 * primary:   '>'  number
 *            '>=' number
 *            '<'  number
 *            ...
 *            'string'
 *            '(' or_expr ')'                             */

bool 
GetPot::__constraint_check_OR(const std::string& Value, const char** iterator) const
{
    bool result = false;

    do {
        if (**iterator == '|') ++(*iterator);
        while( isspace(**iterator) ) ++(*iterator);  // skip whitespace
        if( __constraint_check_AND(Value, iterator) ) result = true;
        while( isspace(**iterator) ) ++(*iterator);  // skip whitespace
    } while ( **iterator == '|' );

    return result;
}

bool
GetPot::__constrain_check_EQUAL_STRING(const char* viterator, const char** iterator) const
{
    ++(*iterator);
    // Compare strings from single quote to single quote 
    // loop: 'break' means 'not equal'
    while( 1 + 1 == 2 ) {
        if( *viterator != **iterator )  break; // letter in value != letter in iterator
        ++(*iterator); ++viterator; 

        if( *viterator == '\0' ) { 
            if   ( **iterator != '\'' ) break; // value ended before iterator
            return true;
        } 
        else if( **iterator == '\'' ) break; // iterator ended before value
    }
    // iterator must proceed to the place after the single quote '
    while( **iterator != '\'' ) ++(*iterator);
    ++(*iterator);
    return false;
}

bool 
GetPot::__constraint_check_AND(const std::string& Value, const char** iterator) const
{
    bool result = true;
     
    do {
        if (**iterator == '&') ++(*iterator);
        while( isspace(**iterator) ) ++(*iterator);  // skip whitespace
        if( ! __constraint_check_PRIMARY(Value, iterator) ) result = false;
        while( isspace(**iterator) ) ++(*iterator);  // skip whitespace
    } while ( **iterator == '&' );

    return result;
}

bool
GetPot::__constraint_check_PRIMARY(const std::string& Value, 
                                   const char**       iterator) const
{
    enum Operator { 
           EQ           = 0x80,
           GREATER      = 0x01,          // 'or'-ring 0x80 into an operator 
           GREATER_EQ   = EQ | GREATER,  // means adding the 'equal option'.
           LESS         = 0x02, 
           LESS_EQ      = EQ | LESS, 
           NOT          = 0x03, 
           NOT_EQ       = EQ | NOT, 
           DEVISABLE_BY = 0x05,
           VOID     
    } op = VOID;

    while( isspace(**iterator) ) ++(*iterator);  // skip whitespace

    switch ( (*iterator)[0] ) {

    default:   op = EQ;            ++(*iterator);     break;
    case '\0': throw std::runtime_error(GETPOT_STR_REACHED_END_OF_CONSTRAINT + " " +
                                       + GETPOT_STR_VALUE + ": \'" + Value + "\'.");

    case '>':  op = GREATER;       ++(*iterator);     break;
    case '<':  op = LESS;          ++(*iterator);     break;
    case '%':  op = DEVISABLE_BY;  ++(*iterator);     break;
    case '!':  op = NOT;           ++(*iterator);     break;
    case '\'': return __constrain_check_EQUAL_STRING(Value.c_str(), iterator); 
    case '(': {
                  ++(*iterator);
                  bool result = __constraint_check_OR(Value, iterator);
                  while( isspace(**iterator) ) ++(*iterator);  // skip whitespace

                  if( **iterator != ')' ) 
                      throw std::runtime_error(std::string("Error while processing the file \"")
                                               + filename + "\": a bracket is missing in a constraint.");
                  ++(*iterator);
                  return result;
              }
    }

    if( (*iterator)[0] == '=' ) {
        if( op == VOID ) throw std::runtime_error(std::string("Syntax error found in a constraint")
                                                  + "string while processing the file \""
                                                  + filename + "\"."); 
        else {
            op = (Operator) (EQ | op);
            ++(*iterator);
        }
    }
    else if( op == VOID ) throw std::runtime_error(std::string("Syntax error found in a constraint")
                                                   + "string while processing the file \""
                                                   + filename + "\"."); 


   if( op == NOT ) {
           return ! __constraint_check_AND(Value, iterator);
    }

    while( isspace(**iterator) ) ++(*iterator);  // skip whitespace

    // Conversions to double.
    double value_numeric   = -1.0;
    double cmp_numeric     = -1.0;

    int result = 0;
    result = sscanf(Value.c_str(),"%lf", &value_numeric);
    if( result == 0 || result == EOF ) return false;
    result = sscanf(*iterator,"%lf",     &cmp_numeric);
    if( result == 0 || result == EOF ) return false; 

    // Skip until whitespace or closing bracket.
    while( !isspace(**iterator) && (*iterator)[0] != ')' )  ++(*iterator);
    while( isspace(**iterator) ) ++(*iterator);  // skip whitespace

    switch( op ) {
    case GREATER:      return value_numeric >  cmp_numeric;
    case GREATER_EQ:   return value_numeric >= cmp_numeric;
    case LESS:         return value_numeric <  cmp_numeric;
    case LESS_EQ:      return value_numeric <= cmp_numeric;
    case NOT_EQ:       return value_numeric != cmp_numeric;
    case EQ:           return value_numeric == cmp_numeric;
    case DEVISABLE_BY: return (value_numeric / cmp_numeric) == double(int(value_numeric / cmp_numeric));
    case NOT:          throw std::runtime_error(std::string("Syntax error found in a constraint")
                                   + "string while processing the file \""
                                   + filename + "\"."); 
    case VOID:          throw std::runtime_error(std::string("Syntax error found in a constraint")
                                    + "string while processing the file \""
                                    + filename + "\"."); 
    }
    throw std::runtime_error(std::string("Syntax error found in a constraint")
                + "string while processing the file \""
                + filename + "\"."); 
}
///////////////////////////////////////////////////////////////////////////////
// (*) variable class
//.............................................................................
//
__GETPOT_INLINE
GetPot::variable::variable()
{}

__GETPOT_INLINE
GetPot::variable::variable(const variable& That)
{
#ifdef WIN32
    operator=(That);
#else
    GetPot::variable::operator=(That);
#endif
}


__GETPOT_INLINE
GetPot::variable::variable(const std::string& Name, const std::string& Value, const std::string& FieldSeparator)
    : name(Name)
{
    // make a copy of the 'Value'
    take(Value, FieldSeparator);
}

__GETPOT_INLINE const std::string*
GetPot::variable::get_element(unsigned Idx, bool ThrowExceptionF /* = GETPOT_SETTING_THROW_EXCEPTION_ON_DEFAULT */) const
{ 
    if( Idx < value.size() ) return &(value[Idx]); 

    if( ThrowExceptionF ) {
        std::string  msg;
        char         tmp[64];
        msg += std::string(GETPOT_STR_VARIABLE) + "\'";
        msg += name + "\' ";
        GETPOT_SNPRINTF(tmp, 64, "%i", (int)Idx);
        msg += std::string(GETPOT_STR_DOES_NOT_CONTAIN_ELEMENT) + ": \'" + tmp + "\'";
        msg += ".";
        throw std::runtime_error(msg);
    }
    else {
        return 0x0;
    }
}

__GETPOT_INLINE void
GetPot::variable::take(const std::string& Value, const std::string& FieldSeparator)
{
    using namespace std;

    original = Value;

    // separate string by white space delimiters using 'strtok'
    // thread safe usage of strtok (no static members)
#   ifdef GETPOT_STRTOK_3_ARGS
    char* spt = 0;
#   endif
    // make a copy of the 'Value'
    char* copy = new char[Value.length()+1];
    strcpy(copy, Value.c_str());
    char* follow_token = GETPOT_STRTOK(copy, FieldSeparator.c_str(), &spt);
    if( value.size() != 0 ) value.erase(value.begin(), value.end());
    while(follow_token != 0) {
        value.push_back(std::string(follow_token));
        follow_token = GETPOT_STRTOK(NULL, FieldSeparator.c_str(), &spt);
    }

    delete [] copy;
}

__GETPOT_INLINE
GetPot::variable::~variable()
{}

__GETPOT_INLINE GetPot::variable&
GetPot::variable::operator=(const GetPot::variable& That)
{
    if( &That != this) {
        name     = That.name;
        value    = That.value;
        original = That.original;
    }
    return *this;
}

///////////////////////////////////////////////////////////////////////////////
// (*) useful functions
//.............................................................................
//

__GETPOT_INLINE std::string  GetPot::__double2string(const double& Value) const {
    // -- converts a double integer into a string
    char* tmp = new char[128];
    GETPOT_SNPRINTF(tmp, (int)sizeof(char)*128, "%e", Value);
    std::string result(tmp);
    delete [] tmp;
    return result;
}

__GETPOT_INLINE std::string  GetPot::__int2string(const int& Value) const {
    // -- converts an integer into a string
    char* tmp = new char[128];
    GETPOT_SNPRINTF(tmp, (int)sizeof(char)*128, "%i", Value);
    std::string result(tmp);
    delete [] tmp;
    return result;
}

__GETPOT_INLINE STRING_VECTOR GetPot::__get_section_tree(const std::string& FullPath) {
    // -- cuts a variable name into a tree of sub-sections. this is requested for recording
    //    requested sections when dealing with 'ufo' detection.
    STRING_VECTOR   result;
    for(ptrdiff_t i=0; i<FullPath.length(); ++i) {
        if( FullPath[i] == '/' ) { 
            result.push_back(FullPath.substr(0, i));
        }
    }

    return result;
}

#undef victorate

#ifdef GETPOT_SETTING_NAMESPACE
} // namespace GetPotNamespace.
#endif



#endif // __INCLUDE_GUARD_GETPOT_CPP__



