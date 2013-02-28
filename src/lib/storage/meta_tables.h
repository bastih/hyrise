#ifndef SRC_LIB_STORAGE_META_TABLES_H_
#define SRC_LIB_STORAGE_META_TABLES_H_

#include "boost/mpl/vector.hpp"
#include "boost/mpl/at.hpp"
#include "boost/mpl/int.hpp"
#include "boost/mpl/size.hpp"
#include "storage/table_types.h"

namespace hyrise { namespace storage {

typedef boost::mpl::vector<RawTable<> > table_types;

typedef AbstractTable base_class;

template <typename... T>
using options = boost::mpl::vector<T...>;

template<typename T>
struct shared_pointer_wrap {
  typedef std::shared_ptr<T> wrapped_type;
  typedef T type;
  template <typename BaseType>
  static wrapped_type cast(BaseType base) {
    return std::dynamic_pointer_cast<type>(base);
  }
};

template<typename T>
struct pointer_wrap {
  typedef T* wrapped_type;
  typedef T type;
  template <typename BaseType>
  static wrapped_type cast(BaseType base) {
    return dynamic_cast<wrapped_type>(base);
  }
};

/*
  This is a simple implementation of a list based type switch. Based on the main
  defintion of all available types this template defintion recurses through to
  find the correct type and based on this type call the functor
*/
template <typename L, typename BaseCase = base_class, template<typename T> class wrap_policy = pointer_wrap, int N = 0, bool Stop=(N == boost::mpl::size<L>::value)>
struct table_type_switch;

template <typename L, typename BaseCase, template<typename T> class wrap_policy, int N>
struct table_type_switch<L, BaseCase, wrap_policy, N, false> {

  template<class F>
  inline auto operator()(typename wrap_policy<BaseCase>::wrapped_type table,
                         F &functor) -> decltype(functor.template operator()<typename boost::mpl::at_c<L, N>::type>()) {
    typedef typename boost::mpl::at_c<L, N>::type current_type;
    typedef typename wrap_policy<current_type>::wrapped_type current_wrapped_type;
    if (const auto& r = wrap_policy<current_type>::cast(table)) {
      return functor.template operator()<current_wrapped_type>();
    } else {
      return table_type_switch<L, BaseCase, wrap_policy, N + 1>()(table, functor);
    }
  }
};

template <typename L, typename BaseCase, template<typename T> class wrap_policy, int N>
struct table_type_switch<L, BaseCase, wrap_policy, N, true> {
  template<class F>
  inline auto operator()(typename wrap_policy<BaseCase>::wrapped_type table,
                         F &functor) -> decltype(functor.template operator()<typename boost::mpl::at_c<L, N>::type>()) {
    throw std::runtime_error("Table Type does not exist");
  }
};


}}

#endif 
