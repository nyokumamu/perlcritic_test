package Onepi::Model::Base;

use strict;
use GameConf;

use base qw/
Class::Accessor
Class::Data::Inheritable
/;
use Carp qw/croak/;
use List::MoreUtils qw/any first_index all/;
use SQL::Abstract;
use SQL::Abstract::Plugin::InsertMulti;
use UNIVERSAL::require;
use DA;
use Onepi::DA;
use Onepi::Util qw/returning part_to_hash_ref debug_log debug_trace stack_trace/;

#my $TABLE_NAME_PREFIX = 'gcard_';

__PACKAGE__->mk_classdata($_) for qw/
db
table
columns
primary_columns
sharding_column
sharding_dao
update_ignore_columns
sql_abstract_obj
/;

__PACKAGE__->sql_abstract_obj(SQL::Abstract->new());

sub schema {
    my $class = shift;
    my %options = @_;
    my ($db, $table, $columns, $primary_columns, $sharding_column, $sharding_dao, $update_ignore_columns) =
        @options{qw/db table columns primary_columns sharding_column sharding_dao update_ignore_columns/};

    croak __PACKAGE__ . '&schema is called' if $class eq __PACKAGE__;

#    $table ||= ($TABLE_NAME_PREFIX . Onepi::Util::underscore((split '::',$class)[-1]));
    $primary_columns ||= +[qw/id/];
    $update_ignore_columns ||= +[];
    unshift(@$update_ignore_columns, @$primary_columns);

    $class->_db_accessor($db);
    $class->_table_accessor($table);
    $class->_columns_accessor($columns);
    $class->_primary_columns_accessor($primary_columns);
    $class->_sharding_column_accessor($sharding_column);
    $class->_sharding_dao_accessor($sharding_dao);
    $class->_update_ignore_columns_accessor($update_ignore_columns);

    $class->follow_best_practice();
    $class->mk_accessors(@{$class->columns()});
}

#TODO : primary の check
sub new {
    my $class = shift;
    my $arg = shift;
    my %options = @_;
    my ($mode, $shard_id) = @options{qw/mode shard_id/};

    my @given_columns = grep { exists $arg->{$_} } @{$class->columns()};

    my %attr;
    @attr{@given_columns} = @$arg{@given_columns};

    return $class->SUPER::new(+{
        attributes    => \%attr,
        changed_columns => +{},
        mode          => $mode,
        shard_id      => $shard_id,
    });
}

# MEMO: 空オブジェクトをとるメソッド
#       attributesを書き換えるとupdateを打てるようになっているが、
#       打つとき(そういう用途で使う時)は注意が必要
my $SKELETON_DEFAULT_VALUE = 0;
sub create_skeleton {
    my $class = shift;
    my %options = @_;
    my ($mode, $shard_id) = @options{qw/mode shard_id/};

    my %attr;
    for my $column (@{$class->columns}) {
        $attr{$column} = $SKELETON_DEFAULT_VALUE;
    }

    my $instance = $class->SUPER::new(+{
        attributes    => \%attr,
        changed_columns => +{},
        mode          => $mode,
        shard_id      => $shard_id,
    });
    $instance->{is_skeleton_model} = 1;
    return $instance;
}

sub create_virtual {
    my $class = shift;
    my ($attr, %options) = @_;
    my ($mode, $shard_id) = @options{qw/mode shard_id/};

    my $instance = $class->SUPER::new(+{
        attributes    => $attr,
        changed_columns => +{},
        mode          => $mode,
        shard_id      => $shard_id,
    });
    $instance->{is_virtual_model} = 1;
    return $instance;
}


sub number_of_pk {
    my $class = shift;
    return scalar @{$class->primary_columns()};
}

sub get_shard_id_by_dao {
    my $class = shift;
    my ($key, %options) = @_;
    return undef unless $class->sharding_dao();
    $class->sharding_dao()->require();
    return $class->sharding_dao()->select_by_id($key, %options);
}
sub get_shard_id_by_shard_key {
    my $class = shift;
    my ($key, %options) = @_;
    return unless Onepi::DA::is_sharding($class->db());
    croak('shard_key is not defined') unless defined $key;
    my $id = $class->get_shard_id_by_dao($key, %options);
    $id ||= Onepi::DA::get_shard_id_by_user_id($key);
    croak("not found shard_id. key:${key}") unless defined $id;
    return $id;
}
sub get_logshard_id_by_shard_key {
    my $class = shift;
    my $key = shift;
    return unless Onepi::DA::is_log_sharding($class->db());
    croak('shard_key is not defined') unless defined $key;
    my $id = Onepi::DA::get_shard_id_by_user_id($key);
    croak("not found shard_id. key:${key}") unless defined $id;
    return Onepi::DA::get_logshard_id_by_shard_id($id, $class->db());
}

###########################################################
# instance methods
#

####################################################
# changed 系
#
sub changed_columns {
    my $self = shift;
    return sort grep { $self->is_changed($_) } keys %{$self->{changed_columns}};
}

sub is_changed {
    my $self = shift;
    my $column = shift;
    return $self->{changed_columns}->{$column} > 0;
}

sub any_changed {
    my $self = shift;
    return any { $self->is_changed($_) } keys %{$self->{changed_columns}};
}

sub add_changed {
    my $self = shift;
    my $column = shift;
    $self->{changed_columns}->{$column}++;
}

sub reset_changed {
    my $self = shift;
    $self->{changed_columns} = +{};
}

sub is_skeleton_model {
    my $self = shift;
    return (defined $self->{is_skeleton_model}) ? 1 : 0;
}
sub is_virtual_model {
    my $self = shift;
    return (defined $self->{is_virtual_model}) ? 1 : 0;
}

#####################################################

sub shard_id {
    my $self = shift;
    return $self->{shard_id};
}

sub mode {
    my $self = shift;
    return $self->{mode};
}

sub get {
    my $self = shift;
    my @columns = @_;

    for (@columns) {
        unless (exists $self->{attributes}->{$_}) {
            croak "${_} is not given when &new";
        }
    }

    return @{$self->{attributes}}{@columns};
}

sub set {
    my $self = shift;
    my ($column,$new_value) = @_;

    return if $self->get($column) eq $new_value;

    $self->add_changed($column);

    $self->{attributes}->{$column} = $new_value;
}

sub attributes {
    my $self = shift;
    my ($from, $to) = ($self->{attributes}, +{});
    @{%$to}{keys %$from} = values %$from;
    return $to;
}

sub class {
    my $self = shift;
    return ref $self;
}

sub primary_keys {
    my $self = shift;
    return +[ $self->get(@{$self->class()->primary_columns()}) ];
}

sub validation {
    my($self) = @_;
    # is abstract method
    my @errors;
    # 全てのカラムが0,nullなら落とす
    unless (any { $self->attributes->{$_} } keys %{$self->attributes}) {
        push @errors, 'All columns are null or 0. Check blank line)';
    }
    return @errors;
}

###################################################################
#
# Query 発行method
#
sub get_handle {
    my $class = shift;
    my ($mode, $shard_id) = @_;
    return Onepi::DA::get_handle($class->db(), $mode, $shard_id);
}

sub get_my_handle {
    my $self = shift;
    return $self->class()->get_handle($self->mode(), $self->shard_id());
}

sub get_seq {
    my $class = shift;
    return Onepi::DA::get_seq($class->table());
}

sub get_seq_multi {
    my $class = shift;
    my ($num) = @_;
    return Onepi::DA::get_seq_multi($class->table(), $num);
}

sub select {
    my $class = shift;
    my @pks = @_[0 .. $class->number_of_pk() - 1];
    my %options = @_[$class->number_of_pk() .. scalar(@_) - 1];

    if (!$options{shard_key} && !$options{shard_id} ) {
        $options{shard_key} = returning {
            return unless $class->sharding_column();
            my $pos = first_index {
                $_ eq $class->sharding_column()
            } @{$class->primary_columns()};
            croak ('not given shard key') if $pos == -1;
            return $pks[$pos];
        };
    }
    my $results = $class->select_by_condition(
        +[join(' AND ', map { "$_ = ?" } @{$class->primary_columns()}), @pks], %options);

    return scalar(@$results) > 0 ? $results->[0] : undef;
}

# multi_pkに対応してないので、select_or_build_for_multi_pksを使うこと
sub select_or_build {
    my($class, $primary_keys, %option) = @_;
    $option{mode} ||= 'R';
    $option{for_update} ||= 0;
    $option{gap_check_mode} ||= 'W';

    # next-key-lock 対策
    # - lock をとらずに master の存在チェックし、あれば lock する
    # - REPEATABLE-READ であったとしても FOR UPDATE したら commited な値を取得できるので問題ない。
    my $selectable = 1;
    if ($option{mode} eq 'W' && $option{for_update}) {
        my $tmp = $class->select($primary_keys, %option, mode => $option{gap_check_mode}, for_update => 0);
        unless ($tmp) {
            $selectable = 0;
        }
    }

    my $self;
    if ($selectable) {
        $self = $class->select($primary_keys, %option);
    }

    unless ($self) {
        $self = $class->build_default($primary_keys, %option);
    }

    return $self;
}

# Model::Base::select_or_buildにprimary_keysをarrayrefで渡すとこけるっぽいのでそれを修正したver
# 他のところではarrayで渡してるところが無いので、修正verはとりあえずここだけにとどめる。
sub select_or_build_for_multi_pks {
    my $class = shift;
    my ($primary_keys, %option) = @_;
    $option{mode} ||= 'R';
    $option{for_update} ||= 0;
    $option{gap_check_mode} ||= 'W';

    # next-key-lock 対策
    # - lock をとらずに master の存在チェックし、あれば lock する
    # - REPEATABLE-READ であったとしても FOR UPDATE したら commited な値を取得できるので問題ない。
    my $selectable = 1;
    if ($option{mode} eq 'W' && $option{for_update}) {
        my $tmp = $class->select(@$primary_keys, %option, mode => $option{gap_check_mode}, for_update => 0);
        unless ($tmp) {
            $selectable = 0;
        }
    }

    my $self;
    if ($selectable) {
        $self = $class->select(@$primary_keys, %option);
    }

    unless ($self) {
        $self = $class->build_default(@$primary_keys, %option);
    }

    return $self;
}



1;
