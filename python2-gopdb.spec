%{!?python_sitelib: %global python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib()"
)}

%define proj_name gopdb

%define _release RELEASEVERSION

Name:           python-%{proj_name}
Version:        RPMVERSION
Release:        %{_release}%{?dist}
Summary:        simpleutil copy from openstack
Group:          Development/Libraries
License:        MPLv1.1 or GPLv2
URL:            http://github.com/Lolizeppelin/%{proj_name}
Source0:        %{proj_name}-%{version}.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildArch:      noarch

BuildRequires:  python-setuptools >= 11.0

Requires:       python >= 2.6.6
Requires:       python < 3.0
Requires:       python-goperation >= 1.0
Requires:       python-goperation < 1.1

%description
utils for update database resource

%prep
%setup -q -n %{proj_name}-%{version}
rm -rf %{proj_name}.egg-info

%build
%{__python} setup.py build

%install
%{__rm} -rf %{buildroot}
%{__python} setup.py install -O1 --skip-build --root %{buildroot}
install -d %{buildroot}%{_sysconfdir}/goperation/endpoints
install -p -D -m 0644 etc/endpoints/*.conf.sample %{buildroot}%{_sysconfdir}/goperation/endpoints

install -d %{buildroot}%{_sbindir}
install -p -D -m 0754 bin/* %{buildroot}%{_sbindir}


%clean
%{__rm} -rf %{buildroot}


%files
%defattr(-,root,root,-)
%{python_sitelib}/%{proj_name}/*.py
%{python_sitelib}/%{proj_name}/*.pyc
%{python_sitelib}/%{proj_name}/*.pyo
%{python_sitelib}/%{proj_name}/api/*.py
%{python_sitelib}/%{proj_name}/api/*.pyc
%{python_sitelib}/%{proj_name}/api/*.pyo
%{python_sitelib}/%{proj_name}/api/client
%{python_sitelib}/%{proj_name}/cmd
%{python_sitelib}/%{proj_name}-%{version}-py?.?.egg-info
%{_sbindir}/%{proj_name}-db-init
%doc README.md
%doc doc/*


%package server
Summary:        Goperation database wsgi routes
Group:          Development/Libraries
Requires:       %{name} == %{version}
Requires:       python-goperation-server >= 1.0
Requires:       python-goperation-server < 1.1


%description server
Goperation database wsgi routes

%files server
%defattr(-,root,root,-)
%dir %{python_sitelib}/%{proj_name}/api/wsgi
%{python_sitelib}/%{proj_name}/api/wsgi/*
%{_sysconfdir}/goperation/endpoints/gopdb.server.conf.sample


%package agent
Summary:        Goperation database rpc agent
Group:          Development/Libraries
Requires:       %{name} == %{version}
Requires:       python-goperation-application >= 1.0
Requires:       python-goperation-application < 1.1
Requires:       mysql >= 5.1.7

%description agent
Goperation database rpc agent

%files agent
%defattr(-,root,root,-)
%dir %{python_sitelib}/%{proj_name}/api/rpc
%{python_sitelib}/%{proj_name}/api/rpc/*
%{_sysconfdir}/goperation/endpoints/gopdb.agent.conf.sample


%changelog

* Mon Aug 29 2017 Lolizeppelin <lolizeppelin@gmail.com> - 1.0.0
- Initial Package