import clsx from 'clsx';
import { ButtonHTMLAttributes } from 'react';

interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: 'primary' | 'ghost' | 'outline';
}

export const Button = ({ variant = 'primary', className, ...props }: ButtonProps) => {
  return (
    <button
      className={clsx(
        'inline-flex items-center justify-center gap-2 rounded-full px-4 py-2 text-sm font-semibold transition',
        variant === 'primary' && 'bg-slate-700 text-slate-100 hover:bg-slate-800',
        variant === 'ghost' && 'text-slate-700 hover:bg-slate-100',
        variant === 'outline' && 'border border-slate-300 text-slate-700 hover:bg-slate-100',
        className,
      )}
      {...props}
    />
  );
};
