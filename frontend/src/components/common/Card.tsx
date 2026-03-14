import clsx from 'clsx';
import { HTMLAttributes } from 'react';

export const Card = ({ className, ...props }: HTMLAttributes<HTMLDivElement>) => {
  return (
    <div
      className={clsx('rounded-2xl border border-slate-200 bg-surface shadow-card', className)}
      {...props}
    />
  );
};
